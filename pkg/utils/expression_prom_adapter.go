package utils

import (
	"fmt"
	"regexp"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/prometheus-adapter/pkg/client"
	"sigs.k8s.io/prometheus-adapter/pkg/config"
	"sigs.k8s.io/prometheus-adapter/pkg/naming"
)

type MetricNamerPromAdapter struct {
	SeriesQuery  client.Selector
	MetricsQuery naming.MetricsQuery
	SeriesName   string
	MetricName   string
}

// NamersFromConfig produces a MetricNamer for each rule in the given config.
func NamersFromConfig(cfg []config.DiscoveryRule, mapper apimeta.RESTMapper) ([]MetricNamerPromAdapter, error) {
	namers := make([]MetricNamerPromAdapter, len(cfg))

	for i, rule := range cfg {
		resConv, err := naming.NewResourceConverter(rule.Resources.Template, rule.Resources.Overrides, mapper)
		if err != nil {
			return nil, err
		}

		// queries are namespaced by default unless the rule specifically disables it
		namespaced := true
		if rule.Resources.Namespaced != nil {
			namespaced = *rule.Resources.Namespaced
		}

		metricsQuery, err := naming.NewExternalMetricsQuery(rule.MetricsQuery, resConv, namespaced)
		if err != nil {
			return nil, fmt.Errorf("unable to construct metrics query associated with series query %q: %v", rule.SeriesQuery, err)
		}

		seriesMatchers := make([]*naming.ReMatcher, len(rule.SeriesFilters))
		for i, filterRaw := range rule.SeriesFilters {
			matcher, err := naming.NewReMatcher(filterRaw)
			if err != nil {
				return nil, fmt.Errorf("unable to generate series name filter associated with series query %q: %v", rule.SeriesQuery, err)
			}
			seriesMatchers[i] = matcher
		}
		if rule.Name.Matches != "" {
			matcher, err := naming.NewReMatcher(config.RegexFilter{Is: rule.Name.Matches})
			if err != nil {
				return nil, fmt.Errorf("unable to generate series name filter from name rules associated with series query %q: %v", rule.SeriesQuery, err)
			}
			seriesMatchers = append(seriesMatchers, matcher)
		}

		var nameMatches *regexp.Regexp
		if rule.Name.Matches != "" {
			nameMatches, err = regexp.Compile(rule.Name.Matches)
			if err != nil {
				return nil, fmt.Errorf("unable to compile series name match expression %q associated with series query %q: %v", rule.Name.Matches, rule.SeriesQuery, err)
			}
		} else {
			// this will always succeed
			nameMatches = regexp.MustCompile(".*")
		}
		nameAs := rule.Name.As
		if nameAs == "" {
			// check if we have an obvious default
			subexpNames := nameMatches.SubexpNames()
			if len(subexpNames) == 1 {
				// no capture groups, use the whole thing
				nameAs = "$0"
			} else if len(subexpNames) == 2 {
				// one capture group, use that
				nameAs = "$1"
			} else {
				return nil, fmt.Errorf("must specify an 'as' value for name matcher %q associated with series query %q", rule.Name.Matches, rule.SeriesQuery)
			}
		}

		// get seriesName
		var seriesName string
		arraySeries := strings.Split(rule.SeriesQuery, "{")
		if arraySeries[0] != "" {
			seriesName = arraySeries[0]
		} else {
			arrayLabel := strings.Split(arraySeries[1], ",")
			for _, label := range arrayLabel {
				compileRegex := regexp.MustCompile("__name__=\"(.*)\"")
				matchArr := compileRegex.FindStringSubmatch(label)
				if len(matchArr) > 0 {
					_, err := regexp.Compile(matchArr[len(matchArr)-1])
					if err != nil {
						return nil, fmt.Errorf("unable to compile series name match expression %q associated with series query %q: %v", rule.Name.Matches, rule.SeriesQuery, err)
					}
					seriesName = matchArr[len(matchArr)-1]
					break
				} else {
					continue
				}
			}
		}

		// get MetricName
		matches := nameMatches.FindStringSubmatchIndex(seriesName)
		if matches == nil {
			return nil, fmt.Errorf("series name %q did not match expected pattern %q", seriesName, nameMatches.String())
		}
		outNameBytes := nameMatches.ExpandString(nil, nameAs, seriesName, matches)
		metricName := string(outNameBytes)

		namer := MetricNamerPromAdapter{
			SeriesQuery:  client.Selector(rule.SeriesQuery),
			MetricsQuery: metricsQuery,
			SeriesName:   seriesName,
			MetricName:   metricName,
		}

		namers[i] = namer
	}

	return namers, nil
}

func (n *MetricNamerPromAdapter) QueryForSeriesCustomer(series string, namespace string, metricSelector labels.Selector) (expressionQuery string, err error) {
	selector, err := n.MetricsQuery.BuildExternal(series, namespace, "", []string{}, metricSelector)
	reg, err := regexp.Compile(` by \(\)$`)
	if err != nil {
		return "", err
	}

	return reg.ReplaceAllString(string(selector), ""), err
}

func (n *MetricNamerPromAdapter) QueryForSeriesExternal(series string, namespace string, metricSelector labels.Selector) (expressionQuery string, err error) {
	selector, err := n.MetricsQuery.BuildExternal(series, namespace, "", []string{}, metricSelector)
	return string(selector), err
}
