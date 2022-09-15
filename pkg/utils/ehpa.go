package utils

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	autoscalingapi "github.com/gocrane/api/autoscaling/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2beta2"

	"github.com/gocrane/crane/pkg/known"
)

func IsEHPAPredictionEnabled(ehpa *autoscalingapi.EffectiveHorizontalPodAutoscaler) bool {
	return ehpa.Spec.Prediction != nil && ehpa.Spec.Prediction.PredictionWindowSeconds != nil && ehpa.Spec.Prediction.PredictionAlgorithm != nil
}

func IsEHPAHasPredictionMetric(ehpa *autoscalingapi.EffectiveHorizontalPodAutoscaler) bool {
	for _, metric := range ehpa.Spec.Metrics {
		if metric.Type == autoscalingv2.ResourceMetricSourceType {
			metricName := GetPredictionMetricName(metric.Type, false)
			if len(metricName) == 0 {
				continue
			}
			return true
		}
	}

	for key := range ehpa.Annotations {
		if strings.HasPrefix(key, known.EffectiveHorizontalPodAutoscalerAnnotationMetricQuery) {
			return true
		}
	}
	return false
}

func IsEHPACronEnabled(ehpa *autoscalingapi.EffectiveHorizontalPodAutoscaler) bool {
	return len(ehpa.Spec.Crons) > 0
}

// GetPredictionMetricName return metric name used by prediction
func GetPredictionMetricName(sourceType autoscalingv2.MetricSourceType, isCron bool) (metricName string) {
	if isCron {
		metricName = known.MetricNameCron
	} else {
		switch sourceType {
		case autoscalingv2.ResourceMetricSourceType, autoscalingv2.PodsMetricSourceType, autoscalingv2.ExternalMetricSourceType:
			metricName = known.MetricNamePrediction
		}
	}

	return metricName
}

// GetGeneralPredictionMetricName return metric name used by prediction
func GetMetricIdentifier(metric autoscalingv2.MetricSpec, name string) string {
	var prefix string
	switch metric.Type {
	case autoscalingv2.PodsMetricSourceType:
		prefix = "pods"
	case autoscalingv2.ResourceMetricSourceType:
		prefix = "resource"
	case autoscalingv2.ExternalMetricSourceType:
		prefix = "external"
	}

	return fmt.Sprintf("%s.%s", prefix, name)
}

// GetExpressionQueryAnnocation return metric query from annotation by metricName
func GetExpressionQueryAnnocation(metricIdentifier string, annotations map[string]string) string {
	for k, v := range annotations {
		if strings.HasPrefix(k, known.EffectiveHorizontalPodAutoscalerAnnotationMetricQuery) {
			compileRegex := regexp.MustCompile(fmt.Sprintf("%s(.*)", known.EffectiveHorizontalPodAutoscalerAnnotationMetricQuery))
			matchArr := compileRegex.FindStringSubmatch(k)
			if len(matchArr) == 2 && matchArr[1][1:] == metricIdentifier {
				return v
			}
		}
	}

	return ""
}

func IsExpressionQueryAnnocationEnabled(metricIdentifier string, annotations map[string]string) bool {
	for k := range annotations {
		if strings.HasPrefix(k, known.EffectiveHorizontalPodAutoscalerAnnotationMetricQuery) {
			compileRegex := regexp.MustCompile(fmt.Sprintf("%s(.*)", known.EffectiveHorizontalPodAutoscalerAnnotationMetricQuery))
			matchArr := compileRegex.FindStringSubmatch(k)
			if len(matchArr) == 2 && matchArr[1][1:] == metricIdentifier {
				return true
			}
		}
	}

	return false
}

// GetExpressionQuery return metric query
func GetExpressionQueryDefault(metric autoscalingv2.MetricSpec, namespace string, name string) string {
	var expressionQuery string
	switch metric.Type {
	case autoscalingv2.ResourceMetricSourceType:
		switch metric.Resource.Name {
		case "cpu":
			expressionQuery = GetWorkloadCpuUsageExpression(namespace, name)
		case "memory":
			expressionQuery = GetWorkloadMemUsageExpression(namespace, name)
		}
	case autoscalingv2.PodsMetricSourceType:
		var labels []string
		if metric.Pods.Metric.Selector != nil {
			if len(metric.Pods.Metric.Selector.MatchLabels) > 0 {
				for k, v := range metric.Pods.Metric.Selector.MatchLabels {
					labels = append(labels, k+"="+`"`+v+`"`)
				}
				expressionQuery = GetCustumerExpression(metric.Pods.Metric.Name, strings.Join(labels, ","))
				return expressionQuery
			}
		}
		expressionQuery = GetCustumerExpression(metric.Pods.Metric.Name, "")
	case autoscalingv2.ExternalMetricSourceType:
		var labels []string
		if metric.External.Metric.Selector != nil {
			if len(metric.External.Metric.Selector.MatchLabels) > 0 {
				for k, v := range metric.External.Metric.Selector.MatchLabels {
					labels = append(labels, k+"="+`"`+v+`"`)
				}
				expressionQuery = GetCustumerExpression(metric.External.Metric.Name, strings.Join(labels, ","))
				return expressionQuery
			}
		}
		expressionQuery = GetCustumerExpression(metric.External.Metric.Name, "")
	}

	return expressionQuery
}

// GetAnnocationPromAdapter return value from annotation by suffix
func GetAnnocationPromAdapter(identifier string, annotations map[string]string) string {
	for k, v := range annotations {
		if strings.HasPrefix(k, known.EffectiveHorizontalPodAutoscalerAnnotationPromAdapter) {
			compileRegex := regexp.MustCompile(fmt.Sprintf("%s(.*)", known.EffectiveHorizontalPodAutoscalerAnnotationPromAdapter))
			matchArr := compileRegex.FindStringSubmatch(k)
			if len(matchArr) == 2 && matchArr[1][1:] == identifier {
				return v
			}
		}
	}

	return ""
}

// GetMatchLabelsByPromAdapterAnnocation return match labels by annotation for prometheus adapter
func GetMatchLabelsByPromAdapterAnnocation(annotations map[string]string, matchLabels map[string]string) (map[string]string, error) {
	value := GetAnnocationPromAdapter(known.PromAdapterGlobalLabels, annotations)
	if value == "" {
		return matchLabels, nil
	}

	lbs := []map[string]interface{}{}
	err := json.Unmarshal([]byte(value), &lbs)
	if err != nil {
		return matchLabels, err
	}

	//	for _, lb := range lbs {
	//		for k, _ := range lb {
	//			matchLabels[k] = fmt.Sprintf("%v",lb[k])
	//		}
	//	}
	//	return matchLabels, nil

	// Avoid the effect of DeepCopy on the original labels
	ls := make(map[string]string)
	for k := range matchLabels {
		ls[k] = fmt.Sprintf("%v", matchLabels[k])
	}

	for _, lb := range lbs {
		for k := range lb {
			ls[k] = fmt.Sprintf("%v", lb[k])
		}
	}
	return ls, nil
}
