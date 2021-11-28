package twitter

import (
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/rs/zerolog/log"

	twitterstream "github.com/fallenstedt/twitter-stream"
)

type StreamData struct {
	Data struct {
		Text      string    `json:"text"`
		ID        string    `json:"id"`
		CreatedAt time.Time `json:"created_at"`
	} `json:"data"`
	MatchingRules []struct {
		ID  string `json:"id"`
		Tag string `json:"tag"`
	} `json:"matching_rules"`
}

type Rule struct {
	Tag   string `yaml:"tag"`
	Value string `yaml:"value"`
}

type Rules []Rule

type rawRules struct {
	Rules Rules `yaml:"rules"`
}

func (r Rules) GetValues() []string {
	var vals []string
	for _, rule := range r {
		vals = append(vals, rule.Value)
	}
	return vals
}

func (r Rules) GetTags() []string {
	var tags []string
	for _, rule := range r {
		tags = append(tags, rule.Tag)
	}
	return tags
}

func generateAddRulesBody(rules Rules) string {
	var body strings.Builder
	body.WriteString("{\"add\": [")
	for index, rule := range rules {
		body.WriteString(fmt.Sprintf("{\"value\": \"%s\", \"tag\": \"%s\"}", rule.Value, rule.Tag))
		if index != (len(rules) - 1) {
			body.WriteString(",")
		}
	}
	body.WriteString("]}")
	return body.String()
}

func ReadRulesFromFile(filePath string) (Rules, error) {
	yamlData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading YAML file: %w", err)
	}

	var rules rawRules
	err = yaml.Unmarshal(yamlData, &rules)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling YAML data into []Rule: %w", err)
	}

	return rules.Rules, nil
}

func AddRules(api *twitterstream.TwitterApi, rules Rules, dryrun bool) error {
	body := generateAddRulesBody(rules)
	fmt.Println(body)
	res, err := api.Rules.AddRules(body, dryrun)
	if err != nil {
		return fmt.Errorf("unable to add rules: %w", err)
	}

	if res.Errors != nil && len(res.Errors) > 0 {
		for _, err := range res.Errors {
			if err.Title == "DuplicateRule" {
				log.Warn().Msgf("attempting to recreate duplicate rule with value %s. Skipping...", err.Value)
			} else {
				log.Error().Msgf("encountered non duplicate rule error %s when adding rules", err.Title)
				return fmt.Errorf("error generating rule with Title: %s and value: %s", err.Title, err.Value)
			}
		}
	}

	log.Info().Msgf("Successfully created %d rules", res.Meta.Summary.Created)
	return nil
}

func DeleteAllRules(api *twitterstream.TwitterApi) error {
	log.Info().Msg("getting existing rules")
	rulesResponse, err := api.Rules.GetRules()
	if err != nil {
		return fmt.Errorf("unable to get rules from Twitter API: %w", err)
	}
	if len(rulesResponse.Data) == 0 {
		log.Info().Msg("found no existing rules, skipping delete")
		return nil
	}
	log.Info().Msgf("found %d existing rules, deleting......", len(rulesResponse.Data))

	var ids []string
	for _, rule := range rulesResponse.Data {
		ids = append(ids, rule.Id)
	}
	err = DeleteRules(api, ids, false)
	if err != nil {
		return fmt.Errorf("unable to delete rules: %w", err)
	}
	return nil
}


func DeleteRules(api *twitterstream.TwitterApi, ruleIDs []string, dryrun bool) error {
	postBody := fmt.Sprintf(`{
	"delete": {
			"ids": %s
		}
	}`, strings.Join(ruleIDs, ","))

	// There is no separate DeleteRules functionality yet in the twitterstream library. AddRules is more a POST rules
	res, err := api.Rules.AddRules(postBody, dryrun)
	if err != nil {
		return fmt.Errorf("unable to delete rules: %w", err)
	}

	if res.Errors != nil && len(res.Errors) > 0 {
		log.Error().Msgf("encountered errors when deleting rules: %v", res.Errors)
		return fmt.Errorf("error deleting rules %v: %s", ruleIDs, res.Errors)
	}

	log.Info().Msgf("Successfully deleted %d rules", res.Meta.Summary.Created)
	return nil
}
