package serve

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"golang.org/x/sync/errgroup"
)

var (
	bucket    = os.Getenv("BUCKET")
	endpoint  = os.Getenv("ENDPOINT")
	accessID  = os.Getenv("ACCESS_KEY_ID")
	accessKey = os.Getenv("ACCESS_KEY_SECRET")
)

const (
	metaContentLength       = "Content-Length"
	metaContentType         = "Content-Type"
	metaDockerContentDigest = "Docker-Content-Digest"
)

func Blob(w http.ResponseWriter, r *http.Request, name string) {
	url := fmt.Sprintf("https://%s.%s/blobs/%s", bucket, endpoint, name)
	http.Redirect(w, r, url, http.StatusSeeOther)
}

type Storage struct {
	client *oss.Client
}

func NewStorage(ctx context.Context) (*Storage, error) {
	if endpoint == "" {
		endpoint = "oss-cn-beijing.aliyuncs.com"
	}
	if bucket == "" {
		bucket = "nydus-demo"
	}

	ossEndpoint := fmt.Sprintf("https://%s", endpoint)
	client, err := oss.New(ossEndpoint, accessID, accessKey)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %v", err)
	}
	return &Storage{client}, nil
}

func (s *Storage) BlobExists(ctx context.Context, name string) (v1.Descriptor, error) {
	bucket, err := s.client.Bucket(bucket)
	if err != nil {
		return v1.Descriptor{}, err
	}
	fmt.Println("get bucket: ", bucket)
	objMetadata, err := bucket.GetObjectDetailedMeta(fmt.Sprintf("blobs/%s", name))
	if err != nil {
		return v1.Descriptor{}, err
	}
	fmt.Printf("get objMetadata: %+v\n", objMetadata)

	var h v1.Hash
	if d := objMetadata["X-Oss-Meta-"+metaDockerContentDigest]; len(d) == 1 {
		h, err = v1.NewHash(d[0])
		if err != nil {
			return v1.Descriptor{}, err
		}
	}

	var size int64 = 0
	if d := objMetadata[metaContentLength]; len(d) == 1 {
		size, err = strconv.ParseInt(d[0], 10, 64)
		if err != nil {
			return v1.Descriptor{}, err
		}
		fmt.Printf("get size: %+v\n", size)
	}

	return v1.Descriptor{
		Digest:    h,
		MediaType: types.MediaType(objMetadata[metaContentType][0]),
		Size:      size,
	}, nil
}

// FIXME only used in cmd/wait/main.go
func (s *Storage) WriteObject(ctx context.Context, name, contents string) error {
	bucket, err := s.client.Bucket(bucket)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("blobs/%s", name)
	return bucket.PutObject(key, strings.NewReader(contents))
}

func (s *Storage) writeBlob(ctx context.Context, name string, h v1.Hash, rc io.ReadCloser, contentType string) error {
	start := time.Now()
	defer func() { log.Printf("writeBlob(%q) took %s", name, time.Since(start)) }()

	bucket, err := s.client.Bucket(bucket)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("blobs/%s", name)

	options := []oss.Option{
		oss.ContentType(contentType),
		oss.Meta(metaContentType, contentType),
		oss.Meta(metaDockerContentDigest, h.String()),
	}

	err = bucket.PutObject(key, rc, options...)
	if err != nil {
		// FIXME: handle already exist error
		return err
	}

	if err := rc.Close(); err != nil {
		return fmt.Errorf("rc.Close: %v", err)
	}
	return nil
}

// ServeIndex writes manifest, config and layer blobs for each image in the
// index, then writes and redirects to the index manifest contents pointing to
// those blobs.
func (s *Storage) ServeIndex(w http.ResponseWriter, r *http.Request, idx v1.ImageIndex, also ...string) error {
	ctx := r.Context()
	im, err := idx.IndexManifest()
	if err != nil {
		return err
	}
	var g errgroup.Group
	for _, m := range im.Manifests {
		m := m
		g.Go(func() error {
			img, err := idx.Image(m.Digest)
			if err != nil {
				return err
			}
			return s.WriteImage(ctx, img)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Write the manifest as a blob.
	b, err := idx.RawManifest()
	if err != nil {
		return err
	}
	mt, err := idx.MediaType()
	if err != nil {
		return err
	}
	digest, err := idx.Digest()
	if err != nil {
		return err
	}
	if err := s.writeBlob(ctx, digest.String(), digest, ioutil.NopCloser(bytes.NewReader(b)), string(mt)); err != nil {
		return err
	}

	for _, a := range also {
		a := a
		g.Go(func() error {
			return s.writeBlob(ctx, a, digest, ioutil.NopCloser(bytes.NewReader(b)), string(mt))
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// If it's just a HEAD request, serve that.
	if r.Method == http.MethodHead {
		s, err := idx.Size()
		if err != nil {
			return err
		}
		w.Header().Set(metaDockerContentDigest, digest.String())
		w.Header().Set(metaContentType, string(mt))
		w.Header().Set(metaContentLength, fmt.Sprintf("%d", s))
		return nil
	}

	// Redirect to manifest blob.
	Blob(w, r, digest.String())
	return nil
}

// WriteImage writes the layer blobs, config blob and manifest.
func (s *Storage) WriteImage(ctx context.Context, img v1.Image, also ...string) error {
	// Write config blob for later serving.
	ch, err := img.ConfigName()
	if err != nil {
		return err
	}
	cb, err := img.RawConfigFile()
	if err != nil {
		return err
	}
	if err := s.writeBlob(ctx, ch.String(), ch, ioutil.NopCloser(bytes.NewReader(cb)), "application/json"); err != nil {
		return err
	}

	// Write layer blobs for later serving.
	layers, err := img.Layers()
	if err != nil {
		return err
	}
	var g errgroup.Group
	for _, l := range layers {
		l := l
		g.Go(func() error {
			rc, err := l.Compressed()
			if err != nil {
				return err
			}
			lh, err := l.Digest()
			if err != nil {
				return err
			}
			mt, err := l.MediaType()
			if err != nil {
				return err
			}
			return s.writeBlob(ctx, lh.String(), lh, rc, string(mt))
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Write the manifest as a blob.
	b, err := img.RawManifest()
	if err != nil {
		return err
	}
	mt, err := img.MediaType()
	if err != nil {
		return err
	}
	digest, err := img.Digest()
	if err != nil {
		return err
	}
	if err := s.writeBlob(ctx, digest.String(), digest, ioutil.NopCloser(bytes.NewReader(b)), string(mt)); err != nil {
		return err
	}
	for _, a := range also {
		a := a
		g.Go(func() error {
			return s.writeBlob(ctx, a, digest, ioutil.NopCloser(bytes.NewReader(b)), string(mt))
		})
	}
	return g.Wait()

}

// ServeManifest writes config and layer blobs for the image, then writes and
// redirects to the image manifest contents pointing to those blobs.
func (s *Storage) ServeManifest(w http.ResponseWriter, r *http.Request, img v1.Image, also ...string) error {
	ctx := r.Context()
	if err := s.WriteImage(ctx, img, also...); err != nil {
		return err
	}

	digest, err := img.Digest()
	if err != nil {
		return err
	}

	// If it's just a HEAD request, serve that.
	if r.Method == http.MethodHead {
		mt, err := img.MediaType()
		if err != nil {
			return err
		}
		s, err := img.Size()
		if err != nil {
			return err
		}
		w.Header().Set(metaDockerContentDigest, digest.String())
		w.Header().Set(metaContentType, string(mt))
		w.Header().Set(metaContentLength, fmt.Sprintf("%d", s))
		return nil
	}

	// Redirect to manifest blob.
	Blob(w, r, digest.String())
	return nil
}
