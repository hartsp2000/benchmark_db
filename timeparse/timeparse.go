package timeparse

import (
	"regexp"
	"strconv"
	"time"
)

func ParseDuration(str string) time.Duration {
	durationRegex := regexp.MustCompile(`(?P<seconds>\d+s)?(?P<milliseconds>\d+ms)?(?P<nanoseconds>\d+ns)?`)
	matches := durationRegex.FindStringSubmatch(str)

	seconds := ParseInt64(matches[1])
	milliseconds := ParseInt64(matches[2])
	nanoseconds := ParseInt64(matches[3])

	second := int64(time.Second)
	millisecond := int64(time.Millisecond)
	nanosecond := int64(time.Nanosecond)

	return time.Duration(seconds*second + milliseconds*millisecond + nanoseconds*nanosecond)
}

func ParseInt64(value string) int64 {
	if len(value) == 0 {
		return 0
	}

	stripRegex := regexp.MustCompile("[0-9]+")
	stringValue := stripRegex.FindAllString(value, -1)

	value = stringValue[0]

	parsed, err := strconv.Atoi(value[:len(value)])
	if err != nil {
		return 0
	}
	return int64(parsed)
}
