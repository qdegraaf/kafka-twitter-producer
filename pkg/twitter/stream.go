package twitter

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	twitterstream "github.com/fallenstedt/twitter-stream"
	"github.com/qdegraaf/kafka-twitter-producer/config"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type StreamData struct {
	Data struct {
		Text      string    `json:"text"`
		ID        string    `json:"id"`
		CreatedAt time.Time `json:"created_at"`
		AuthorID  string    `json:"author_id"`
	} `json:"data"`
	Includes struct {
		Users []struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			Username string `json:"username"`
		} `json:"users"`
	} `json:"includes"`
	MatchingRules []struct {
		ID  string  `json:"id"`
		Tag string `json:"tag"`
	} `json:"matching_rules"`
}

func GetClient(config config.Config) *twitter.Client {
	// oauth2 configures a client that uses app credentials to keep a fresh token
	conf := &clientcredentials.Config{
		ClientID:     config.TwitterAPIKey,
		ClientSecret: config.TwitterAPISecret,
		TokenURL:     config.TwitterTokenURL,
	}

	// http.Client will automatically authorize Requests
	httpClient := conf.Client(oauth2.NoContext)

	// Twitter client
	client := twitter.NewClient(httpClient)
	return client
}

func AddRules(config config.Config, rules string, dryrun bool) error {
	tok, err := twitterstream.NewTokenGenerator().SetApiKeyAndSecret(config.TwitterAPIKey, config.TwitterAPISecret).RequestBearerToken()
	if err != nil {
		log.Fatal().Msgf("Could not get Twitter bearer token " + err.Error())
	}
	api := twitterstream.NewTwitterStream(tok.AccessToken)
	res, err := api.Rules.AddRules(rules, dryrun)
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

	log.Info().Msgf("Succesfully created %d rules", res.Meta.Summary.Created)
	return nil
}
