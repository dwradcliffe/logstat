package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/redis.v3"

	"github.com/jehiah/go-strftime"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
)

// "<134>2015-08-24T02:23:27Z cache-iad2134 downloads[484670]: 108.227.232.148 Mon, 24 Aug 2015 02:23:25 GMT /staging.s3.rubygems.org/gems/dwradcliffe_test_gem_push-0.0.47.gem 304"
// "<134>2015-08-24T12:44:59Z cache-lhr6335 downloads[332933]: 54.72.251.121 Mon, 24 Aug 2015 12:44:59 GMT /production.s3.rubygems.org/gems/multi_xml-0.5.5.gem 200 Ruby, RubyGems/2.0.14 x86_64-linux Ruby/2.0.0 (2015-04-13 patchlevel 645)"
var LogRegex = regexp.MustCompile(`^<134>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\scache-[a-z]{3}[0-9]{4}\sdownloads\[[0-9]+\]:\s((?:[0-9]{1,3}\.){3}[0-9]{1,3})\s(.*)\sGMT\s/(?:staging|production).s3.rubygems.org/gems/(.*)\.gem\s[0-9]{3}\s(.*)$`)

// "dwradcliffe_test_gem_push-0.0.47"
var NameRegex = regexp.MustCompile(`^(.*)-(.*)$`)

// "RubyGems/1.8.17 x86-linux Ruby/1.8.7 (2010-12-23 patchlevel 330)"
// "Ruby, RubyGems/2.0.14 x86_64-linux Ruby/2.0.0 (2015-04-13 patchlevel 645)"
var UserAgentRegex = regexp.MustCompile(`^Ruby, RubyGems/([0-9\.]+)\s(.*)\sRuby/(.*)\s\((.*)\)$`)

type Download struct {
	GemName          string
	FullGemName      string
	RubygemsVersion  string
	RubygemsPlatform string
	RubyVersion      string
	RubyRelease      string
}

func parseLogLine(logLine string) (Download, error) {
	logParts := LogRegex.FindAllStringSubmatch(logLine, 4)[0]
	// TODO: sometimes these regexs fail and we need to handle that
	nameParts := NameRegex.FindAllStringSubmatch(logParts[3], 1)[0]
	parsed := Download{
		GemName:     nameParts[1],
		FullGemName: logParts[3],
	}
	userAgent := logParts[4]
	if UserAgentRegex.MatchString(userAgent) {
		userAgentParts := UserAgentRegex.FindAllStringSubmatch(userAgent, 4)[0]
		parsed.RubygemsVersion = userAgentParts[1]
		parsed.RubygemsPlatform = userAgentParts[2]
		parsed.RubyVersion = userAgentParts[3]
		parsed.RubyRelease = userAgentParts[4]
	}
	return parsed, nil
}

func save(data Download, client *redis.Client) {
	today := strftime.Format("%Y-%m-%d", time.Now())
	client.Incr("downloads").Err()
	client.Incr(fmt.Sprintf("downloads:rubygem:%s", data.GemName)).Err()
	client.Incr(fmt.Sprintf("downloads:version:%s", data.FullGemName)).Err()
	client.ZIncrBy(fmt.Sprintf("downloads:today:%s", today), 1, data.FullGemName).Err()
	client.ZIncrBy("downloads:all", 1, data.FullGemName).Err()
	client.HIncrBy(fmt.Sprintf("downloads:version_history:%s", data.FullGemName), today, 1).Err()
	client.HIncrBy(fmt.Sprintf("downloads:rubygem_history:%s", data.GemName), today, 1).Err()

	if data.RubygemsVersion != "" {
		client.HIncrBy(fmt.Sprintf("usage:rubygem_version:%s", today), data.RubygemsVersion, 1).Err()
	}
	if data.RubygemsPlatform != "" {
		client.HIncrBy(fmt.Sprintf("usage:ruby_platform:%s", today), data.RubygemsPlatform, 1).Err()
	}
	if data.RubyVersion != "" {
		client.HIncrBy(fmt.Sprintf("usage:ruby_version:%s", today), data.RubyVersion, 1).Err()
	}
	if data.RubyRelease != "" {
		client.HIncrBy(fmt.Sprintf("usage:ruby_release:%s", today), data.RubyRelease, 1).Err()
	}
}

func main() {
	fmt.Println("RubyGems.org stats processer!")

	// config
	bucketName := "rubygems-fastly-download-logs"
	var redisHost string
	var redisPort string
	var environment string
	flag.StringVar(&redisHost, "redisHost", "localhost", "redis host")
	flag.StringVar(&redisPort, "redisPort", "6379", "redis port")
	flag.StringVar(&environment, "environment", "production", "environment (staging/production)")
	flag.Parse()

	// setup redis connection
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisHost + ":" + redisPort,
		Password: "",
		DB:       5,
	})
	err := redisClient.Ping().Err()
	if err != nil {
		fmt.Println("Redis Error: ")
		fmt.Println(err)
		os.Exit(1)
	} else {
		fmt.Printf("Connected to redis on %s:%s.\n", redisHost, redisPort)
	}

	// setup s3 client
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Fatal(err)
	}
	s3Client := s3.New(auth, aws.USWest2)

	bucket := s3Client.Bucket(bucketName)
	prefix := environment + "/2015-08-24T12:26"
	fmt.Printf("Looking for log files starting with `%s`\n", prefix)
	resp, err := bucket.List(prefix, "", "", 1000)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range resp.Contents {
		log.Printf("Found %s, processing...", file.Key)
		data, err := bucket.Get(file.Key)
		if err != nil {
			log.Fatal(err)
		}
		logLines := strings.Split(string(data), "\n")

		for _, logLine := range logLines {
			if logLine == "" {
				break
			}
			parsed, err := parseLogLine(logLine)
			if err != nil {
				log.Println(err)
			}
			log.Printf("%+v", parsed)
			save(parsed, redisClient)
		}

	}

}
