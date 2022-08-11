package certmagic_azblob

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type CaddyAzblob struct {
	logger        *zap.Logger
	AccountName   string `json:"account_name"`
	AccountKey    string `json:"account_key"`
	ContainerName string `json:"container_name"`
	UUID          string
	ContainerURL  azblob.ContainerURL
}

func init() {
	caddy.RegisterModule(CaddyAzblob{})
}

func (blob *CaddyAzblob) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		var value string

		key := d.Val()

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "account_name":
			blob.AccountName = value
		case "account_key":
			blob.AccountKey = value
		case "container_name":
			blob.ContainerName = value
		}
	}

	return nil
}

func (blob *CaddyAzblob) Provision(ctx caddy.Context) error {
	blob.logger = ctx.Logger(blob)

	//Used for lock ownership, each caddy process must have its own uuid
	blob.UUID = uuid.NewString()

	// Load Environment
	if blob.AccountName == "" {
		blob.AccountName = os.Getenv("AZBLOB_ACCOUNT_NAME")
	}

	if blob.AccountKey == "" {
		blob.AccountKey = os.Getenv("AZBLOB_ACCOUNT_KEY")
	}

	if blob.ContainerName == "" {
		blob.ContainerName = os.Getenv("AZBLOB_ACCOUNT_CONTAINER_NAME")
	}

	creds, err := azblob.NewSharedKeyCredential(blob.AccountName, blob.AccountKey)
	if err != nil {
		panic(err)
	}

	p := azblob.NewPipeline(creds, azblob.PipelineOptions{})
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", blob.AccountName))
	if err != nil {
		return err
	}

	serviceURL := azblob.NewServiceURL(*u, p)
	containerURL := serviceURL.NewContainerURL(blob.ContainerName)

	blob.ContainerURL = containerURL
	return nil
}

func (CaddyAzblob) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.azblob",
		New: func() caddy.Module {
			return new(CaddyAzblob)
		},
	}
}

func (blob CaddyAzblob) CertMagicStorage() (certmagic.Storage, error) {
	return blob, nil
}

func (blob CaddyAzblob) Lock(ctx context.Context, key string) error {
	// blob.logger.Info("Lock", zap.String("key", key))
	// blobURL := blob.ContainerURL.NewBlockBlobURL(key)
	// _, err := blobURL.AcquireLease(context.TODO(), blob.UUID, -1, azblob.ModifiedAccessConditions{})
	// if err != nil {
	// 	blob.logger.Error("Lock Error", zap.String("err", err.Error()))
	// }
	// return err
	return nil
}

func (blob CaddyAzblob) Unlock(ctx context.Context, key string) error {
	// blob.logger.Info("Unlock", zap.String("key", key))
	// blobURL := blob.ContainerURL.NewBlockBlobURL(key)
	// _, err := blobURL.AcquireLease(context.TODO(), blob.UUID, -1, azblob.ModifiedAccessConditions{})
	// //_, err := blob.ContainerURL.ReleaseLease(ctx, blob.UUID, azblob.ModifiedAccessConditions{})
	// if err != nil {
	// 	blob.logger.Error("Unlock Error", zap.String("err", err.Error()))
	// }
	// return err
	return nil
}

func (blob CaddyAzblob) Store(ctx context.Context, key string, value []byte) error {
	blobURL := blob.ContainerURL.NewBlockBlobURL(key)
	_, err := blobURL.Upload(ctx, bytes.NewReader(value), azblob.BlobHTTPHeaders{ContentType: "text/plain"}, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, nil, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})
	if err != nil {
		blob.logger.Error("Store Error", zap.String("err", err.Error()))
	}
	return err
}

func (blob CaddyAzblob) Load(ctx context.Context, key string) ([]byte, error) {
	blobURL := blob.ContainerURL.NewBlockBlobURL(key)
	get, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if !DoesBlobExists(err) {
		blob.logger.Error("Load Error", zap.String("err", err.Error()))
		return nil, fs.ErrNotExist
	}

	if err != nil {
		blob.logger.Error("Load Error", zap.String("err", err.Error()))
		return nil, fs.ErrNotExist
	}
	downloadedData := &bytes.Buffer{}
	reader := get.Body(azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(reader)
	if err != nil {
		blob.logger.Error("Load Error", zap.String("err", err.Error()))
		return nil, err
	}
	return downloadedData.Bytes(), err
}

func (blob CaddyAzblob) Delete(ctx context.Context, key string) error {
	blobURL := blob.ContainerURL.NewBlockBlobURL(key)
	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		blob.logger.Error("Delete Error", zap.String("err", err.Error()))
	}

	return err
}

func (blob CaddyAzblob) Exists(ctx context.Context, key string) bool {
	items, err := blob.List(ctx, key, true)
	if err != nil {
		blob.logger.Error("Exists", zap.String("err", err.Error()))
		return false
	}

	for _, v := range items {
		if strings.Contains(v, key) {
			blob.logger.Error("Exists", zap.Bool("exists", true), zap.String("key", key))
			return true
		}
	}

	return false
}

func (blob CaddyAzblob) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	blob.logger.Info("List", zap.String("prefix", prefix))
	ls, err := blob.ContainerURL.ListBlobsFlatSegment(context.TODO(), azblob.Marker{}, azblob.ListBlobsSegmentOptions{})
	if err != nil {
		blob.logger.Error("List Error", zap.String("err", err.Error()))
		return nil, err
	}

	keys := make([]string, 0)
	for _, v := range ls.Segment.BlobItems {
		keys = append(keys, v.Name)
	}

	blob.logger.Error("List Keys", zap.String("list", strings.Join(keys, ",")))
	return keys, nil
}

func (blob CaddyAzblob) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	blob.logger.Info("Stat", zap.String("key", key))
	blobURL := blob.ContainerURL.NewBlockBlobURL(key)
	resp, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil && !DoesBlobExists(err) {
		blob.logger.Error("Stat", zap.String("err", err.Error()))
		return certmagic.KeyInfo{}, err
	}

	data, err := blob.Load(ctx, key)
	if err != nil {
		blob.logger.Error("Stat 2", zap.String("err", err.Error()))
		return certmagic.KeyInfo{}, err
	}

	return certmagic.KeyInfo{
		Key:        key,
		Modified:   resp.LastModified(),
		Size:       int64(len(data)), //TODO: Not sure how to get size from azblob.BlobGetPropertiesResponse
		IsTerminal: true,
	}, err
}

func DoesBlobExists(err error) bool {
	if err == nil {
		return true
	}
	return !strings.Contains(err.Error(), "404 The specified blob does not exist")
}

func (blob CaddyAzblob) String() string {
	return fmt.Sprintf("AZBlob Account Name: %s, Container Name: %s", blob.AccountName, blob.ContainerName)
}
