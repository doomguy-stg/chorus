/*
 * Copyright © 2025 Strato GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package migration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestConsistency_Trailingslash(t *testing.T) {
	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)

	// Create first bucket with two objects. Both objects have the same name, but one has a trailing slash appended
	err := mainClient.MakeBucket(ctx, "trailingslashsrc", minio.MakeBucketOptions{})
	r.NoError(err)
	if err != nil {
		return
	}
	defer mainClient.RemoveBucket(ctx, "trailingslashsrc")
	_, err = mainClient.PutObject(ctx, "trailingslashsrc", "object/", strings.NewReader("foo"), int64(len("foo")),
		minio.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	if err != nil {
		return
	}
	defer mainClient.RemoveObject(ctx, "trailingslashsrc", "object/", minio.RemoveObjectOptions{})
	_, err = mainClient.PutObject(ctx, "trailingslashsrc", "object", strings.NewReader("foo"), int64(len("foo")),
		minio.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	if err != nil {
		return
	}
	defer mainClient.RemoveObject(ctx, "trailingslashsrc", "object", minio.RemoveObjectOptions{})

	// Create second bucket with two objects with identical names. The object with the trailing slash will have the
	// same content as in the first bucket, the object without trailing slash will differ from the first bucket
	err = f1Client.MakeBucket(ctx, "trailingslashdst", minio.MakeBucketOptions{})
	r.NoError(err)
	if err != nil {
		return
	}
	defer f1Client.RemoveBucket(ctx, "trailingslashdst")
	_, err = f1Client.PutObject(ctx, "trailingslashdst", "object", strings.NewReader("bar"), int64(len("bar")),
		minio.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	if err != nil {
		return
	}
	defer f1Client.RemoveObject(ctx, "trailingslashdst", "object", minio.RemoveObjectOptions{})
	_, err = f1Client.PutObject(ctx, "trailingslashdst", "object/", strings.NewReader("foo"), int64(len("foo")),
		minio.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	if err != nil {
		return
	}
	defer f1Client.RemoveObject(ctx, "trailingslashdst", "object/", minio.RemoveObjectOptions{})

	locations := []*pb.MigrateLocation{
		{
			Storage: "main",
			Bucket:  "trailingslashsrc",
			User:    user,
		},
		{
			Storage: "f1",
			Bucket:  "trailingslashdst",
			User:    user,
		},
	}

	checkRequest := &pb.ConsistencyCheckRequest{
		Locations: locations,
	}
	_, err = apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)
	if err != nil {
		return
	}
	defer apiClient.DeleteConsistencyCheckReport(ctx, checkRequest)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
		if err != nil {
			return false
		}
		if getCheckResponse == nil {
			return false
		}
		if getCheckResponse.Check == nil {
			return false
		}
		if !getCheckResponse.Check.Ready {
			return false
		}
		return true
	}, 10*time.Second, 500*time.Millisecond)

	fmt.Printf("%+v\n", getCheckResponse)
	checkEntries, err := apiClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
		Locations: locations,
		PageSize:  10,
	})
	r.NoError(err)
	fmt.Printf("%+v\n", checkEntries)

	// the check should report an inconsistency
	r.False(getCheckResponse.Check.Consistent)
}

func TestConsistency_Lostsofslashes(t *testing.T) {
	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)

	// create object name with 250 slashes
	objname := fmt.Sprintf("aaa%s", strings.Repeat("/aaa", 250))

	// create first bucket and object tith previously created name
	err := mainClient.MakeBucket(ctx, "lotofslashessrc", minio.MakeBucketOptions{})
	r.NoError(err)
	if err != nil {
		return
	}
	defer mainClient.RemoveBucket(ctx, "lotofslashessrc")
	_, err = mainClient.PutObject(ctx, "lotofslashessrc", objname, strings.NewReader("foo"), int64(len("foo")),
		minio.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	if err != nil {
		return
	}
	defer mainClient.RemoveObject(ctx, "lotofslashessrc", objname, minio.RemoveObjectOptions{})

	// create second bucket with the same object
	err = f1Client.MakeBucket(ctx, "lotofslashesdst", minio.MakeBucketOptions{})
	r.NoError(err)
	if err != nil {
		return
	}
	defer f1Client.RemoveBucket(ctx, "lotofslashesdst")
	_, err = f1Client.PutObject(ctx, "lotofslashesdst", objname, strings.NewReader("foo"), int64(len("foo")),
		minio.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	if err != nil {
		return
	}
	defer f1Client.RemoveObject(ctx, "lotofslashesdst", objname, minio.RemoveObjectOptions{})

	locations := []*pb.MigrateLocation{
		{
			Storage: "main",
			Bucket:  "lotofslashessrc",
			User:    user,
		},
		{
			Storage: "f1",
			Bucket:  "lotofslashesdst",
			User:    user,
		},
	}

	checkRequest := &pb.ConsistencyCheckRequest{
		Locations: locations,
	}
	_, err = apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)
	if err != nil {
		return
	}
	defer apiClient.DeleteConsistencyCheckReport(ctx, checkRequest)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse

	// Checking two objects should finish within 10 seconds
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
		if err != nil {
			return false
		}
		if getCheckResponse == nil {
			return false
		}
		if getCheckResponse.Check == nil {
			return false
		}
		if !getCheckResponse.Check.Ready {
			return false
		}
		return true
	}, 10*time.Second, 500*time.Millisecond)

	// if you increase the timeout above, you see why the current implementation takes so long:
	// the consistency has directory semantics and queues one event per slash. In this case: 502 instead of 2
	r.True(getCheckResponse.Check.Consistent)
	r.Equal(uint64(2), getCheckResponse.Check.Completed)
	r.Equal(uint64(2), getCheckResponse.Check.Queued)
}
