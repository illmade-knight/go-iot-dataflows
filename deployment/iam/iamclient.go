// iamclient.go
package iam

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/iam"
	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"

	"cloud.google.com/go/iam/admin/apiv1"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// Client holds the necessary modern Google Cloud service clients for IAM operations.
type Client struct {
	ctx            context.Context
	iamAdminClient *admin.IamClient
	pubsubClient   *pubsub.Client
	storageClient  *storage.Client
	bigqueryClient *bigquery.Client
}

// NewClient creates a new aggregated client for IAM operations using modern libraries.
func NewClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*Client, error) {
	iamAdminClient, err := admin.NewIamClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create IAM admin client: %w", err)
	}

	pubsubClient, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	storageClient, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud Storage client: %w", err)
	}

	bigqueryClient, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return &Client{
		ctx:            ctx,
		iamAdminClient: iamAdminClient,
		pubsubClient:   pubsubClient,
		storageClient:  storageClient,
		bigqueryClient: bigqueryClient,
	}, nil
}

// Close gracefully closes all underlying clients.
func (c *Client) Close() error {
	var errs []error
	if err := c.iamAdminClient.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close IAM admin client: %w", err))
	}
	if err := c.pubsubClient.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close Pub/Sub client: %w", err))
	}
	if err := c.storageClient.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close Storage client: %w", err))
	}
	if err := c.bigqueryClient.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close BigQuery client: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to close one or more clients: %v", errs)
	}
	return nil
}

// AddPubSubTopicIAMBinding uses the modern pubsub client's IAM handle to manage policies.
func (c *Client) AddPubSubTopicIAMBinding(ctx context.Context, topicID, role, serviceAccountEmail string) error {
	member := "serviceAccount:" + serviceAccountEmail
	log.Info().Str("topic", topicID).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: Granting role")

	topic := c.pubsubClient.Topic(topicID)
	policy, err := topic.IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("failed to get IAM policy for topic %s: %w", topicID, err)
	}

	policy.Add(member, iam.RoleName(role))

	if err := topic.IAM().SetPolicy(ctx, policy); err != nil {
		return fmt.Errorf("failed to set IAM policy for topic %s: %w", topicID, err)
	}

	return nil
}

// RemovePubSubTopicIAMBinding removes an IAM binding from a Pub/Sub topic.
func (c *Client) RemovePubSubTopicIAMBinding(ctx context.Context, topicID, role, serviceAccountEmail string) error {
	member := "serviceAccount:" + serviceAccountEmail
	log.Info().Str("topic", topicID).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: Revoking Pub/Sub Topic role")

	topic := c.pubsubClient.Topic(topicID)
	policy, err := topic.IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("failed to get IAM policy for topic %s: %w", topicID, err)
	}

	policy.Remove(member, iam.RoleName(role))

	if err := topic.IAM().SetPolicy(ctx, policy); err != nil {
		return fmt.Errorf("failed to set IAM policy for topic %s: %w", topicID, err)
	}

	return nil
}

// AddPubSubSubscriptionIAMBinding adds an IAM binding to a Pub/Sub subscription.
func (c *Client) AddPubSubSubscriptionIAMBinding(ctx context.Context, subscriptionID, role, serviceAccountEmail string) error {
	member := "serviceAccount:" + serviceAccountEmail
	log.Info().Str("subscription", subscriptionID).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: Granting Pub/Sub Subscription role")

	sub := c.pubsubClient.Subscription(subscriptionID)
	policy, err := sub.IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("failed to get IAM policy for subscription %s: %w", subscriptionID, err)
	}

	policy.Add(member, iam.RoleName(role))

	if err := sub.IAM().SetPolicy(ctx, policy); err != nil {
		return fmt.Errorf("failed to set IAM policy for subscription %s: %w", subscriptionID, err)
	}

	return nil
}

// RemovePubSubSubscriptionIAMBinding removes an IAM binding from a Pub/Sub subscription.
func (c *Client) RemovePubSubSubscriptionIAMBinding(ctx context.Context, subscriptionID, role, serviceAccountEmail string) error {
	member := "serviceAccount:" + serviceAccountEmail
	log.Info().Str("subscription", subscriptionID).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: Revoking Pub/Sub Subscription role")

	sub := c.pubsubClient.Subscription(subscriptionID)
	policy, err := sub.IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("failed to get IAM policy for subscription %s: %w", subscriptionID, err)
	}

	policy.Remove(member, iam.RoleName(role))

	if err := sub.IAM().SetPolicy(ctx, policy); err != nil {
		return fmt.Errorf("failed to set IAM policy for subscription %s: %w", subscriptionID, err)
	}

	return nil
}

// AddGCSBucketIAMBinding uses the modern storage client's IAM handle to manage policies.
func (c *Client) AddGCSBucketIAMBinding(ctx context.Context, bucketName, role, serviceAccountEmail string) error {
	member := "serviceAccount:" + serviceAccountEmail
	log.Info().Str("bucket", bucketName).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: Granting GCSBucket role")

	bucket := c.storageClient.Bucket(bucketName)
	policy, err := bucket.IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("failed to get IAM policy for bucket %s: %w", bucketName, err)
	}

	policy.Add(member, iam.RoleName(role))

	if err := bucket.IAM().SetPolicy(ctx, policy); err != nil {
		return fmt.Errorf("failed to set IAM policy for bucket %s: %w", bucketName, err)
	}

	return nil
}

// RemoveGCSBucketIAMBinding removes an IAM binding from a GCS bucket.
func (c *Client) RemoveGCSBucketIAMBinding(ctx context.Context, bucketName, role, serviceAccountEmail string) error {
	member := "serviceAccount:" + serviceAccountEmail
	log.Info().Str("bucket", bucketName).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: Revoking GCS Bucket role")

	bucket := c.storageClient.Bucket(bucketName)
	policy, err := bucket.IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("failed to get IAM policy for bucket %s: %w", bucketName, err)
	}

	policy.Remove(member, iam.RoleName(role))

	if err := bucket.IAM().SetPolicy(ctx, policy); err != nil {
		return fmt.Errorf("failed to set IAM policy for bucket %s: %w", bucketName, err)
	}

	return nil
}

// AddBigQueryDatasetIAMBinding adds an IAM binding to a BigQuery dataset.
// This implementation uses the bigquery client's Access methods and aligns with
// the provided AccessEntry type definitions, including validation for the role.
func (c *Client) AddBigQueryDatasetIAMBinding(ctx context.Context, projectID, datasetID, role, serviceAccountEmail string) error {
	log.Info().Str("dataset", datasetID).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: Granting BigQuery role")

	// Validate the provided role against known BigQuery AccessRoles.
	validRoles := map[string]struct{}{
		string(bigquery.OwnerRole):  {},
		string(bigquery.ReaderRole): {},
		string(bigquery.WriterRole): {},
	}

	if _, ok := validRoles[role]; !ok {
		return fmt.Errorf("invalid BigQuery AccessRole '%s'. Must be one of '%s', '%s', or '%s'",
			role, bigquery.OwnerRole, bigquery.ReaderRole, bigquery.WriterRole)
	}

	dataset := c.bigqueryClient.DatasetInProject(projectID, datasetID)

	meta, err := dataset.Metadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to get metadata for BigQuery dataset %s: %w", datasetID, err)
	}

	// Initializing AccessEntry with IAMMemberEntity and casting role to bigquery.AccessRole.
	newAccessEntry := bigquery.AccessEntry{
		Role:       bigquery.AccessRole(role), // Cast string to bigquery.AccessRole
		EntityType: bigquery.IAMMemberEntity,
		Entity:     serviceAccountEmail,
		View:       nil,
		Routine:    nil,
		Dataset:    nil,
		Condition:  nil,
	}

	// Check if this entry already exists to ensure idempotency.
	found := false
	for _, entry := range meta.Access {
		if entry.Role == newAccessEntry.Role &&
			entry.EntityType == newAccessEntry.EntityType &&
			entry.Entity == newAccessEntry.Entity {
			found = true
			break
		}
	}

	if found {
		log.Info().Str("dataset", datasetID).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: BigQuery access entry already exists.")
		return nil
	}

	update := bigquery.DatasetMetadataToUpdate{
		Access: append(meta.Access, &newAccessEntry),
	}

	if _, err := dataset.Update(ctx, update, meta.ETag); err != nil {
		return fmt.Errorf("failed to update IAM policy for BigQuery dataset %s: %w", datasetID, err)
	}

	log.Info().Str("dataset", datasetID).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: BigQuery role granted successfully.")
	return nil
}

// RemoveBigQueryDatasetIAMBinding removes an IAM binding from a BigQuery dataset.
func (c *Client) RemoveBigQueryDatasetIAMBinding(ctx context.Context, projectID, datasetID, role, serviceAccountEmail string) error {
	log.Info().Str("dataset", datasetID).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: Revoking BigQuery role")

	dataset := c.bigqueryClient.DatasetInProject(projectID, datasetID)

	meta, err := dataset.Metadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to get metadata for BigQuery dataset %s: %w", datasetID, err)
	}

	// Create a new Access slice without the entry to be removed.
	var updatedAccess []*bigquery.AccessEntry
	memberToRemove := bigquery.AccessEntry{
		Role:       bigquery.AccessRole(role),
		EntityType: bigquery.IAMMemberEntity,
		Entity:     serviceAccountEmail,
	}
	removed := false
	for _, entry := range meta.Access {
		// Compare relevant fields. View, Routine, Dataset, Condition are omitted as they might not be present or used for IAMMemberEntity.
		if !(entry.Role == memberToRemove.Role &&
			entry.EntityType == memberToRemove.EntityType &&
			entry.Entity == memberToRemove.Entity) {
			updatedAccess = append(updatedAccess, entry)
		} else {
			removed = true
		}
	}

	if !removed {
		log.Info().Str("dataset", datasetID).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: BigQuery access entry not found for removal. Continuing...")
		return nil // Entry not found, nothing to remove
	}

	update := bigquery.DatasetMetadataToUpdate{
		Access: updatedAccess,
	}

	if _, err := dataset.Update(ctx, update, meta.ETag); err != nil {
		return fmt.Errorf("failed to update IAM policy for BigQuery dataset %s: %w", datasetID, err)
	}

	log.Info().Str("dataset", datasetID).Str("role", role).Str("serviceAccount", serviceAccountEmail).Msg("IAM Provision: BigQuery role revoked successfully.")
	return nil
}

// EnsureServiceAccountExists creates a service account if it does not already exist.
// It uses the modern IAM Admin Client.
func (c *Client) EnsureServiceAccountExists(ctx context.Context, projectID, accountName string) (string, error) {
	accountID := strings.Split(accountName, "@")[0]
	resourceName := fmt.Sprintf("projects/%s/serviceAccounts/%s@%s.iam.gserviceaccount.com", projectID, accountID, projectID)
	email := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", accountID, projectID)

	_, err := c.iamAdminClient.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: resourceName})
	if err == nil {
		log.Printf("IAM check: Service account %s already exists.", email)
		return email, nil
	}

	if status.Code(err) != codes.NotFound {
		return "", fmt.Errorf("failed to check for service account %s: %w", email, err)
	}

	log.Printf("IAM Provision: Creating service account: %s", email)
	createReq := &adminpb.CreateServiceAccountRequest{
		Name:      "projects/" + projectID,
		AccountId: accountID,
		ServiceAccount: &adminpb.ServiceAccount{
			DisplayName: "Service Account for " + accountID,
		},
	}

	sa, err := c.iamAdminClient.CreateServiceAccount(ctx, createReq)
	if err != nil {
		return "", fmt.Errorf("failed to create service account %s: %w", email, err)
	}

	log.Printf("IAM Provision: Successfully created service account %s", sa.Email)
	return sa.Email, nil
}
