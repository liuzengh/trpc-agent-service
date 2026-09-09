package releaseverify

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

var labelNamePattern = regexp.MustCompile(`^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`)
var labelPrefixPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

func validLabelValue(value string) bool {
	return len(value) <= 63 && (value == "" || labelNamePattern.MatchString(value))
}

func validLabelKey(key string) bool {
	name := key
	if prefix, suffix, qualified := strings.Cut(key, "/"); qualified {
		if len(prefix) > 253 || !labelPrefixPattern.MatchString(prefix) {
			return false
		}
		name = suffix
	}
	return name != "" && validLabelValue(name)
}

func (s labelSelector) validate() error {
	for key, value := range s.MatchLabels {
		if !validLabelKey(key) || !validLabelValue(value) {
			return fmt.Errorf("invalid label key or value")
		}
	}
	for _, requirement := range s.MatchExpressions {
		if !validLabelKey(requirement.Key) {
			return fmt.Errorf("invalid expression key")
		}
		switch requirement.Operator {
		case "In", "NotIn":
			if len(requirement.Values) == 0 {
				return fmt.Errorf("%s requires nonempty values", requirement.Operator)
			}
		case "Exists", "DoesNotExist":
			if len(requirement.Values) != 0 {
				return fmt.Errorf("%s requires empty values", requirement.Operator)
			}
		default:
			return fmt.Errorf("unsupported expression operator %q", requirement.Operator)
		}
		for _, value := range requirement.Values {
			if !validLabelValue(value) {
				return fmt.Errorf("invalid expression value")
			}
		}
	}
	return nil
}

// Kubernetes ANDs every requirement; NotIn also matches an absent key.
func (s labelSelector) matches(labels map[string]string) bool {
	for key, expected := range s.MatchLabels {
		if value, exists := labels[key]; !exists || value != expected {
			return false
		}
	}
	for _, requirement := range s.MatchExpressions {
		value, exists := labels[requirement.Key]
		switch requirement.Operator {
		case "In":
			if !exists || !slices.Contains(requirement.Values, value) {
				return false
			}
		case "NotIn":
			if exists && slices.Contains(requirement.Values, value) {
				return false
			}
		case "Exists":
			if !exists {
				return false
			}
		case "DoesNotExist":
			if exists {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// External gateway templates are not in this bundle. Check contradictions on
// their mandatory labels while retaining unknown operator-owned constraints.
func (s labelSelector) admitsRequiredLabel(key, value string) bool {
	if expected, exists := s.MatchLabels[key]; !exists || expected != value {
		return false
	}
	known := labelSelector{}
	for _, requirement := range s.MatchExpressions {
		if requirement.Key == key {
			known.MatchExpressions = append(known.MatchExpressions, requirement)
		}
	}
	return known.matches(map[string]string{key: value})
}
