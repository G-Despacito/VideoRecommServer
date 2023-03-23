package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	fipb "cs426.yale.edu/lab1/failure_injection/proto"
	umc "cs426.yale.edu/lab1/user_service/mock_client"
	usl "cs426.yale.edu/lab1/user_service/server_lib"
	vmc "cs426.yale.edu/lab1/video_service/mock_client"
	vsl "cs426.yale.edu/lab1/video_service/server_lib"

	pb "cs426.yale.edu/lab1/video_rec_service/proto"
	sl "cs426.yale.edu/lab1/video_rec_service/server_lib"
)

func TestServerBasic(t *testing.T) {
	vrOptions := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: true,
		DisableRetry:    true,
	}
	// You can specify failure injection options here or later send
	// SetInjectionConfigRequests using these mock clients
	uClient :=
		umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient :=
		vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions,
		uClient,
		vClient,
	)

	var userId uint64 = 204054
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	assert.True(t, err == nil)

	videos := out.Videos
	assert.Equal(t, 5, len(videos))
	assert.EqualValues(t, 1012, videos[0].VideoId)
	assert.Equal(t, "Harry Boehm", videos[1].Author)
	assert.EqualValues(t, 1209, videos[2].VideoId)
	assert.Equal(t, "https://video-data.localhost/blob/1309", videos[3].Url)
	assert.Equal(t, "precious here", videos[4].Title)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			// fail one in 1 request, i.e., always fail
			FailureRate: 1,
		},
	})

	// Since we disabled retry and fallback, we expect the VideoRecService to
	// throw an error since the VideoService is "down".
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	assert.False(t, err == nil)
}

func TestBatching(t *testing.T) {
	vrOptions := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: true,
		DisableRetry:    true,
	}
	uClient :=
		umc.MakeMockUserServiceClient(usl.UserServiceOptions{
			MaxBatchSize: 10,
		})
	vClient :=
		vmc.MakeMockVideoServiceClient(vsl.VideoServiceOptions{
			MaxBatchSize: 10,
		})
	vrService := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions,
		uClient,
		vClient,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			FailureRate: 0,
		},
	})

	var userId uint64 = 204054
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: userId, Limit: 5},
	)
	assert.False(t, err == nil)
}

func TestStats(t *testing.T) {
	vrOptions := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: true,
		DisableRetry:    true,
	}
	uClient :=
		umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient :=
		vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions,
		uClient,
		vClient,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			FailureRate: 0,
		},
	})

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: 204054, Limit: 5},
	)
	assert.Equal(t, vrService.TotalRequests, uint64(1))
	assert.Equal(t, vrService.TotalResponses, uint64(1))
	assert.Equal(t, vrService.TotalErrors, uint64(0))
	assert.Equal(t, vrService.VideoServiceErrors, uint64(0))
	assert.Equal(t, vrService.StaleResponses, uint64(0))

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			FailureRate: 1,
		},
	})

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: 205000, Limit: 5},
	)
	assert.Equal(t, vrService.TotalRequests, uint64(2))
	assert.Equal(t, vrService.TotalResponses, uint64(2))
	assert.Equal(t, vrService.TotalErrors, uint64(1))
	assert.Equal(t, vrService.VideoServiceErrors, uint64(1))
	assert.Equal(t, vrService.StaleResponses, uint64(0))
}

func TestErrorHandling(t *testing.T) {
	vrOptions := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: true,
		DisableRetry:    true,
	}
	uClient :=
		umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient :=
		vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions,
		uClient,
		vClient,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			FailureRate: 1,
		},
	})

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := vrService.GetTopVideos(
		ctx,
		&pb.GetTopVideosRequest{UserId: 205000, Limit: 5},
	)
	assert.False(t, err == nil)
}

func TestRetrying(t *testing.T) {
	vrOptions1 := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: true,
		DisableRetry:    false,
	}
	uClient1 :=
		umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient1 :=
		vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService1 := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions1,
		uClient1,
		vClient1,
	)

	vrOptions2 := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: true,
		DisableRetry:    true,
	}
	uClient2 :=
		umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient2 :=
		vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService2 := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions2,
		uClient2,
		vClient2,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient1.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			FailureRate: 10,
		},
	})
	vClient2.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			FailureRate: 10,
		},
	})

	for i := 200400; i < 200500; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		vrService1.GetTopVideos(
			ctx,
			&pb.GetTopVideosRequest{UserId: uint64(i), Limit: 5},
		)
		vrService2.GetTopVideos(
			ctx,
			&pb.GetTopVideosRequest{UserId: uint64(i), Limit: 5},
		)
	}

	assert.True(t, vrService1.TotalErrors+3 < vrService2.TotalErrors)
}

func TestFallbackToTrendingVideos(t *testing.T) {
	vrOptions1 := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: false,
		DisableRetry:    true,
	}
	uClient1 :=
		umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient1 :=
		vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService1 := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions1,
		uClient1,
		vClient1,
	)

	vrOptions2 := sl.VideoRecServiceOptions{
		MaxBatchSize:    50,
		DisableFallback: true,
		DisableRetry:    true,
	}
	uClient2 :=
		umc.MakeMockUserServiceClient(*usl.DefaultUserServiceOptions())
	vClient2 :=
		vmc.MakeMockVideoServiceClient(*vsl.DefaultVideoServiceOptions())
	vrService2 := sl.MakeVideoRecServiceServerWithMocks(
		vrOptions2,
		uClient2,
		vClient2,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vClient1.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			FailureRate: 10,
		},
	})
	vClient2.SetInjectionConfig(ctx, &fipb.SetInjectionConfigRequest{
		Config: &fipb.InjectionConfig{
			FailureRate: 10,
		},
	})

	for i := 200400; i < 200500; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		vrService1.GetTopVideos(
			ctx,
			&pb.GetTopVideosRequest{UserId: uint64(i), Limit: 5},
		)

		vrService2.GetTopVideos(
			ctx,
			&pb.GetTopVideosRequest{UserId: uint64(i), Limit: 5},
		)
	}

	assert.True(t, vrService1.TotalErrors+3 < vrService2.TotalErrors)
}
