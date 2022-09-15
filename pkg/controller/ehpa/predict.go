package ehpa

import (
	"context"
	"fmt"

	autoscalingv2 "k8s.io/api/autoscaling/v2beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	autoscalingapi "github.com/gocrane/api/autoscaling/v1alpha1"
	predictionapi "github.com/gocrane/api/prediction/v1alpha1"
	"github.com/gocrane/crane/pkg/known"
	"github.com/gocrane/crane/pkg/utils"
)

func (c *EffectiveHPAController) ReconcilePredication(ctx context.Context, ehpa *autoscalingapi.EffectiveHorizontalPodAutoscaler) (*predictionapi.TimeSeriesPrediction, error) {
	predictionList := &predictionapi.TimeSeriesPredictionList{}
	opts := []client.ListOption{
		client.MatchingLabels(map[string]string{known.EffectiveHorizontalPodAutoscalerUidLabel: string(ehpa.UID)}),
	}
	err := c.Client.List(ctx, predictionList, opts...)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.CreatePrediction(ctx, ehpa)
		} else {
			c.Recorder.Event(ehpa, corev1.EventTypeNormal, "FailedGetPrediction", err.Error())
			klog.Errorf("Failed to get TimeSeriesPrediction, ehpa %s error %v", klog.KObj(ehpa), err)
			return nil, err
		}
	} else if len(predictionList.Items) == 0 {
		return c.CreatePrediction(ctx, ehpa)
	}

	return c.UpdatePredictionIfNeed(ctx, ehpa, &predictionList.Items[0])
}

func (c *EffectiveHPAController) GetPredication(ctx context.Context, ehpa *autoscalingapi.EffectiveHorizontalPodAutoscaler) (*predictionapi.TimeSeriesPrediction, error) {
	predictionList := &predictionapi.TimeSeriesPredictionList{}
	opts := []client.ListOption{
		client.MatchingLabels(map[string]string{known.EffectiveHorizontalPodAutoscalerUidLabel: string(ehpa.UID)}),
	}
	err := c.Client.List(ctx, predictionList, opts...)
	if err != nil {
		return nil, err
	} else if len(predictionList.Items) == 0 {
		return nil, nil
	}

	return &predictionList.Items[0], nil
}

func (c *EffectiveHPAController) CreatePrediction(ctx context.Context, ehpa *autoscalingapi.EffectiveHorizontalPodAutoscaler) (*predictionapi.TimeSeriesPrediction, error) {
	prediction, err := c.NewPredictionObject(ctx, ehpa)
	if err != nil {
		c.Recorder.Event(ehpa, corev1.EventTypeNormal, "FailedCreatePredictionObject", err.Error())
		klog.Errorf("Failed to create object, TimeSeriesPrediction %s error %v", klog.KObj(prediction), err)
		return nil, err
	}

	err = c.Client.Create(ctx, prediction)
	if err != nil {
		c.Recorder.Event(ehpa, corev1.EventTypeNormal, "FailedCreatePrediction", err.Error())
		klog.Errorf("Failed to create TimeSeriesPrediction %s error %v", klog.KObj(prediction), err)
		return nil, err
	}

	klog.Infof("Creation TimeSeriesPrediction %s successfully", klog.KObj(prediction))
	c.Recorder.Event(ehpa, corev1.EventTypeNormal, "PredictionCreated", "Create TimeSeriesPrediction successfully")

	return prediction, nil
}

func (c *EffectiveHPAController) UpdatePredictionIfNeed(ctx context.Context, ehpa *autoscalingapi.EffectiveHorizontalPodAutoscaler, predictionExist *predictionapi.TimeSeriesPrediction) (*predictionapi.TimeSeriesPrediction, error) {
	prediction, err := c.NewPredictionObject(ctx, ehpa)
	if err != nil {
		c.Recorder.Event(ehpa, corev1.EventTypeNormal, "FailedCreatePredictionObject", err.Error())
		klog.Errorf("Failed to create object, TimeSeriesPrediction %s error %v", klog.KObj(prediction), err)
		return nil, err
	}

	if !equality.Semantic.DeepEqual(&predictionExist.Spec, &prediction.Spec) {
		predictionExist.Spec = prediction.Spec
		err := c.Update(ctx, predictionExist)
		if err != nil {
			c.Recorder.Event(ehpa, corev1.EventTypeNormal, "FailedUpdatePrediction", err.Error())
			klog.Errorf("Failed to update TimeSeriesPrediction %s", klog.KObj(predictionExist))
			return nil, err
		}

		klog.Infof("Update TimeSeriesPrediction successful, TimeSeriesPrediction %s", klog.KObj(predictionExist))
	}

	return predictionExist, nil
}

func (c *EffectiveHPAController) NewPredictionObject(ctx context.Context, ehpa *autoscalingapi.EffectiveHorizontalPodAutoscaler) (*predictionapi.TimeSeriesPrediction, error) {
	name := fmt.Sprintf("ehpa-%s", ehpa.Name)
	prediction := &predictionapi.TimeSeriesPrediction{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ehpa.Namespace, // the same namespace to effectivehpa
			Name:      name,
			Labels: map[string]string{
				"app.kubernetes.io/name":                       name,
				"app.kubernetes.io/part-of":                    ehpa.Name,
				"app.kubernetes.io/managed-by":                 known.EffectiveHorizontalPodAutoscalerManagedBy,
				known.EffectiveHorizontalPodAutoscalerUidLabel: string(ehpa.UID),
			},
		},
		Spec: predictionapi.TimeSeriesPredictionSpec{
			PredictionWindowSeconds: *ehpa.Spec.Prediction.PredictionWindowSeconds,
			TargetRef: corev1.ObjectReference{
				Kind:       ehpa.Spec.ScaleTargetRef.Kind,
				Namespace:  ehpa.Namespace,
				Name:       ehpa.Spec.ScaleTargetRef.Name,
				APIVersion: ehpa.Spec.ScaleTargetRef.APIVersion,
			},
		},
	}

	var predictionMetrics []predictionapi.PredictionMetric
	for _, metric := range ehpa.Spec.Metrics {
		var metricName string
		//get metricIdentifier by metric.Type and metricName
		var metricIdentifier string
		switch metric.Type {
		case autoscalingv2.ResourceMetricSourceType:
			switch metric.Resource.Name {
			case "cpu":
				metricIdentifier = utils.GetMetricIdentifier(metric, corev1.ResourceCPU.String())
			case "memory":
				metricIdentifier = utils.GetMetricIdentifier(metric, corev1.ResourceMemory.String())
			}
		case autoscalingv2.ExternalMetricSourceType:
			metricName = metric.External.Metric.Name
			metricIdentifier = utils.GetMetricIdentifier(metric, metric.External.Metric.Name)
		case autoscalingv2.PodsMetricSourceType:
			metricName = metric.Pods.Metric.Name
			metricIdentifier = utils.GetMetricIdentifier(metric, metric.Pods.Metric.Name)
		}

		if metricIdentifier == "" {
			continue
		}

		//get expressionQuery
		var expressionQuery string
		//first get annocation expressionQuery
		expressionQuery = utils.GetExpressionQueryAnnocation(metricIdentifier, ehpa.Annotations)
		if expressionQuery == "" {
			// second get prometheus-adatper expressionQuery
			switch metric.Type {
			case autoscalingv2.ExternalMetricSourceType:
				if len(c.NamersExternal) > 0 {
					for _, namer := range c.NamersExternal {
						if metricName == namer.MetricName {
							klog.V(4).Infof("Got namers prometheus-adapter-external MetricName[%s] SeriesName[%s]", namer.MetricName, namer.SeriesName)
							matchLabels, err := utils.GetMatchLabelsByPromAdapterAnnocation(ehpa.Annotations, metric.External.Metric.Selector.MatchLabels)
							if err != nil {
								klog.Errorf("Got matchLabels by prometheus-adapter annocation ehpa[%s] %v", ehpa.Name, err)
							}
							expressionQuery, err = namer.QueryForSeriesExternal(namer.SeriesName, ehpa.Namespace, labels.SelectorFromSet(matchLabels))
							if err != nil {
								klog.Errorf("Got promSelector prometheus-adapter-external %v", err)
							} else {
								klog.V(4).Infof("Got expressionQuery prometheus-adapter-external [%s]", expressionQuery)
							}
						}
					}
				}
			case autoscalingv2.PodsMetricSourceType:
				if len(c.NamersCustomer) > 0 {
					for _, namer := range c.NamersCustomer {
						if metricName == namer.MetricName {
							klog.V(4).Infof("Got namers prometheus-adapter-customer MetricName[%s] SeriesName[%s]", namer.MetricName, namer.SeriesName)
							matchLabels, err := utils.GetMatchLabelsByPromAdapterAnnocation(ehpa.Annotations, metric.Pods.Metric.Selector.MatchLabels)
							if err != nil {
								klog.Errorf("Got matchLabels by prometheus-adapter annocation ehpa[%s] %v", ehpa.Name, err)
							}
							expressionQuery, err = namer.QueryForSeriesCustomer(namer.SeriesName, ehpa.Namespace, labels.SelectorFromSet(matchLabels))
							if err != nil {
								klog.Errorf("Got promSelector prometheus-adapter-customer %v", err)
							} else {
								klog.V(4).Infof("Got expressionQuery prometheus-adapter-customer [%s]", expressionQuery)
							}
						}
					}
				}
			}
			// third get default expressionQuery
			if expressionQuery == "" {
				//if annocation not matched, and configmap is not set, build expressionQuerydefault by metric and ehpa.TargetName
				expressionQuery = utils.GetExpressionQueryDefault(metric, ehpa.Namespace, ehpa.Spec.ScaleTargetRef.Name)
				klog.V(4).Infof("Got expressionQuery default [%s]", expressionQuery)
			}
		} else {
			klog.V(4).Infof("Got expressionQuery annocation [%s]", expressionQuery)
		}

		if expressionQuery == "" {
			continue
		}

		predictionMetrics = append(predictionMetrics, predictionapi.PredictionMetric{
			ResourceIdentifier: metricIdentifier,
			Type:               predictionapi.ExpressionQueryMetricType,
			ExpressionQuery: &predictionapi.ExpressionQuery{
				Expression: expressionQuery,
			},
			Algorithm: predictionapi.Algorithm{
				AlgorithmType: ehpa.Spec.Prediction.PredictionAlgorithm.AlgorithmType,
				DSP:           ehpa.Spec.Prediction.PredictionAlgorithm.DSP,
				Percentile:    ehpa.Spec.Prediction.PredictionAlgorithm.Percentile,
			},
		})
	}
	prediction.Spec.PredictionMetrics = predictionMetrics

	// EffectiveHPA control the underground prediction so set controller reference for it here
	if err := controllerutil.SetControllerReference(ehpa, prediction, c.Scheme); err != nil {
		return nil, err
	}

	return prediction, nil
}

func setPredictionCondition(status *autoscalingapi.EffectiveHorizontalPodAutoscalerStatus, conditions []metav1.Condition) {
	for _, cond := range conditions {
		if cond.Type == string(predictionapi.TimeSeriesPredictionConditionReady) {
			if len(cond.Reason) > 0 {
				setCondition(status, autoscalingapi.PredictionReady, cond.Status, cond.Reason, cond.Message)
			}
		}
	}
}
