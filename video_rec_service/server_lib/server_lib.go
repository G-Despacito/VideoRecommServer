package server_lib

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"cs426.yale.edu/lab1/ranker"
	umc "cs426.yale.edu/lab1/user_service/mock_client"
	upb "cs426.yale.edu/lab1/user_service/proto"
	pb "cs426.yale.edu/lab1/video_rec_service/proto"
	vmc "cs426.yale.edu/lab1/video_service/mock_client"
	vpb "cs426.yale.edu/lab1/video_service/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"sync/atomic"

	"github.com/influxdata/tdigest"
)

type VideoRecServiceOptions struct {
	// Server address for the UserService"
	UserServiceAddr string
	// Server address for the VideoService
	VideoServiceAddr string
	// Maximum size of batches sent to UserService and VideoService
	MaxBatchSize int
	// If set, disable fallback to cache
	DisableFallback bool
	// If set, disable all retries
	DisableRetry bool
	// Size of the UserServiceClient/VideoServiceClient pool
	ClientPoolSize int
}

type StoreTrendingVideos struct {
	TrendingVideos  []*vpb.VideoInfo
	ExpirationTimeS uint64
	mu              sync.RWMutex
}

type StoreLatencyMss struct {
	td *tdigest.TDigest
	mu sync.RWMutex
}

type VideoRecServiceServer struct {
	pb.UnimplementedVideoRecServiceServer
	options               VideoRecServiceOptions
	TotalRequests         uint64
	TotalErrors           uint64
	TotalResponses        uint64
	UserServiceErrors     uint64
	VideoServiceErrors    uint64
	TotalLatencyMs        uint64
	StaleResponses        uint64
	LatencyMss            StoreLatencyMss
	TrendingVideos        StoreTrendingVideos
	UserServiceConn       []*grpc.ClientConn
	VideoServiceConn      []*grpc.ClientConn
	UserServiceConnIndex  uint64
	VideoServiceConnIndex uint64
}

func MakeVideoRecServiceServer(options VideoRecServiceOptions) (*VideoRecServiceServer, error) {
	UserServiceConn := make([]*grpc.ClientConn, options.ClientPoolSize)
	VideoServiceConn := make([]*grpc.ClientConn, options.ClientPoolSize)
	var err error
	for i := 0; i < options.ClientPoolSize; i++ {
		UserServiceConn[i], err = serviceConn(options.UserServiceAddr)
		if err != nil {
			return nil, err
		}
		VideoServiceConn[i], err = serviceConn(options.VideoServiceAddr)
		if err != nil {
			return nil, err
		}
	}
	return &VideoRecServiceServer{
		options:               options,
		TotalRequests:         0,
		TotalErrors:           0,
		TotalResponses:        0,
		UserServiceErrors:     0,
		VideoServiceErrors:    0,
		TotalLatencyMs:        0,
		StaleResponses:        0,
		LatencyMss:            StoreLatencyMss{td: tdigest.New()},
		TrendingVideos:        StoreTrendingVideos{TrendingVideos: make([]*vpb.VideoInfo, 0)},
		UserServiceConn:       UserServiceConn,
		VideoServiceConn:      VideoServiceConn,
		UserServiceConnIndex:  0,
		VideoServiceConnIndex: 0,
	}, nil
}

type MockVideoRecServiceServer struct {
	pb.UnimplementedVideoRecServiceServer
	options            VideoRecServiceOptions
	userServiceClient  *umc.MockUserServiceClient
	videoServiceClient *vmc.MockVideoServiceClient
	TotalRequests      uint64
	TotalErrors        uint64
	TotalResponses     uint64
	UserServiceErrors  uint64
	VideoServiceErrors uint64
	TotalLatencyMs     uint64
	StaleResponses     uint64
	LatencyMss         StoreLatencyMss
	TrendingVideos     StoreTrendingVideos
}

func MakeVideoRecServiceServerWithMocks(
	options VideoRecServiceOptions,
	mockUserServiceClient *umc.MockUserServiceClient,
	mockVideoServiceClient *vmc.MockVideoServiceClient,
) *MockVideoRecServiceServer {
	return &MockVideoRecServiceServer{
		options:            options,
		userServiceClient:  mockUserServiceClient,
		videoServiceClient: mockVideoServiceClient,
		TotalRequests:      0,
		TotalErrors:        0,
		TotalResponses:     0,
		UserServiceErrors:  0,
		VideoServiceErrors: 0,
		TotalLatencyMs:     0,
		StaleResponses:     0,
		LatencyMss:         StoreLatencyMss{td: tdigest.New()},
		TrendingVideos:     StoreTrendingVideos{TrendingVideos: make([]*vpb.VideoInfo, 0)},
	}
}

func (server *MockVideoRecServiceServer) GetTopVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
) (*pb.GetTopVideosResponse, error) {
	atomic.AddUint64(&server.TotalRequests, 1)
	start := time.Now()
	wasError := false
	wasSuccess := false
	wasStale := false
	defer func() {
		atomic.AddUint64(&server.TotalResponses, 1)
		elapsed := uint64(time.Since(start).Milliseconds())
		atomic.AddUint64(&server.TotalLatencyMs, elapsed)
		func() {
			server.LatencyMss.mu.Lock()
			defer server.LatencyMss.mu.Unlock()
			server.LatencyMss.td.Add(float64(elapsed), 1)
		}()
		if !wasSuccess {
			atomic.AddUint64(&server.TotalErrors, 1)
		} else if wasStale {
			atomic.AddUint64(&server.StaleResponses, 1)
		}
	}()

	getTopVideosResponse, err := server.GoGetTopVideos(ctx, req)
	if err != nil {
		if server.options.DisableFallback {
			wasError = true
			errStatus, _ := status.FromError(err)
			return nil, status.Errorf(errStatus.Code(), fmt.Sprintln("Error: can't get top videos"))
		}
		if len(server.TrendingVideos.TrendingVideos) == 0 {
			wasError = true
			errStatus, _ := status.FromError(err)
			return nil, status.Errorf(errStatus.Code(), fmt.Sprintln("Error: no stored trending videos"))
		}
		getTopVideosResponse = &pb.GetTopVideosResponse{
			Videos:        server.TrendingVideos.TrendingVideos[:int(math.Min(float64(req.Limit), float64(len(server.TrendingVideos.TrendingVideos))))],
			StaleResponse: true,
		}
	}

	if !wasError {
		wasStale = getTopVideosResponse.StaleResponse
		wasSuccess = true
	}
	return getTopVideosResponse, nil
}

func (server *MockVideoRecServiceServer) GoGetTopVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
) (*pb.GetTopVideosResponse, error) {
	GetUserResponse, err := server.userServiceClient.GetUser(ctx, &upb.GetUserRequest{
		UserIds: []uint64{req.UserId},
	})
	if err != nil {
		if server.options.DisableRetry {
			atomic.AddUint64(&server.UserServiceErrors, 1)
			return nil, retriveError("user", []uint64{req.UserId}, err)
		}
		GetUserResponse, err = server.userServiceClient.GetUser(ctx, &upb.GetUserRequest{
			UserIds: []uint64{req.UserId},
		})
		if err != nil {
			atomic.AddUint64(&server.UserServiceErrors, 1)
			return nil, retriveError("user", []uint64{req.UserId}, err)
		}
	}
	userCoef := GetUserResponse.Users[0].UserCoefficients

	SubscribedToUsersInfos, err := server.fetchUsersInBatches(GetUserResponse.Users[0].SubscribedTo, server.options.MaxBatchSize)
	if err != nil {
		errStatus, _ := status.FromError(err)
		return nil, status.Errorf(errStatus.Code(), errStatus.Message())
	}

	videosIds := getVideoIdsFromUsers(SubscribedToUsersInfos)

	VideosInfos, err := server.fetchVideosInBatches(videosIds, server.options.MaxBatchSize)
	if err != nil {
		errStatus, _ := status.FromError(err)
		return nil, status.Errorf(errStatus.Code(), errStatus.Message())
	}

	topVideos := RankVideosAndGetTop(userCoef, VideosInfos, req.Limit)

	return &pb.GetTopVideosResponse{Videos: topVideos}, nil
}

func (server *MockVideoRecServiceServer) fetchUsersInBatches(
	userIds []uint64,
	batchSize int,
) ([]*upb.UserInfo, error) {
	wasError := false
	wasSuccess := false
	defer func() {
		if !wasSuccess {
			atomic.AddUint64(&server.UserServiceErrors, 1)
		}
	}()

	var res []*upb.UserInfo
	for i := 0; i < len(userIds); i += server.options.MaxBatchSize {
		j := i + server.options.MaxBatchSize
		if j > len(userIds) {
			j = len(userIds)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		out, err := server.userServiceClient.GetUser(ctx, &upb.GetUserRequest{
			UserIds: userIds[i:j],
		})
		if err != nil {
			if server.options.DisableRetry {
				wasError = true
				return nil, retriveError("user", userIds[i:j], err)
			}
			out, err = server.userServiceClient.GetUser(ctx, &upb.GetUserRequest{
				UserIds: userIds[i:j],
			})
			if err != nil {
				wasError = true
				return nil, retriveError("user", userIds[i:j], err)
			}
		}
		res = append(res, out.Users...)
	}

	if !wasError {
		wasSuccess = true
	}
	return res, nil
}

func (server *MockVideoRecServiceServer) fetchVideosInBatches(
	videoIds []uint64,
	batchSize int,
) ([]*vpb.VideoInfo, error) {
	wasError := false
	wasSuccess := false
	defer func() {
		if !wasSuccess {
			atomic.AddUint64(&server.VideoServiceErrors, 1)
		}
	}()

	var res []*vpb.VideoInfo
	for i := 0; i < len(videoIds); i += server.options.MaxBatchSize {
		j := i + server.options.MaxBatchSize
		if j > len(videoIds) {
			j = len(videoIds)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		out, err := server.videoServiceClient.GetVideo(ctx, &vpb.GetVideoRequest{
			VideoIds: videoIds[i:j],
		})
		if err != nil {
			if server.options.DisableRetry {
				wasError = true
				return nil, retriveError("user", videoIds[i:j], err)
			}
			out, err = server.videoServiceClient.GetVideo(ctx, &vpb.GetVideoRequest{
				VideoIds: videoIds[i:j],
			})
			if err != nil {
				wasError = true
				return nil, retriveError("video", videoIds[i:j], err)
			}
		}
		res = append(res, out.Videos...)
	}

	if !wasError {
		wasSuccess = true
	}
	return res, nil
}

func (server *VideoRecServiceServer) GetTopVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
) (*pb.GetTopVideosResponse, error) {
	atomic.AddUint64(&server.TotalRequests, 1)
	start := time.Now()
	wasError := false
	wasSuccess := false
	wasStale := false
	defer func() {
		atomic.AddUint64(&server.TotalResponses, 1)
		elapsed := uint64(time.Since(start).Milliseconds())
		atomic.AddUint64(&server.TotalLatencyMs, elapsed)
		func() {
			server.LatencyMss.mu.Lock()
			defer server.LatencyMss.mu.Unlock()
			server.LatencyMss.td.Add(float64(elapsed), 1)
		}()
		if !wasSuccess {
			atomic.AddUint64(&server.TotalErrors, 1)
		} else if wasStale {
			atomic.AddUint64(&server.StaleResponses, 1)
		}
	}()

	getTopVideosResponse, err := server.GoGetTopVideos(ctx, req)
	if err != nil {
		if server.options.DisableFallback {
			wasError = true
			errStatus, _ := status.FromError(err)
			return nil, status.Errorf(errStatus.Code(), fmt.Sprintln("Error: can't get top videos"))
		}
		if len(server.TrendingVideos.TrendingVideos) == 0 {
			wasError = true
			errStatus, _ := status.FromError(err)
			return nil, status.Errorf(errStatus.Code(), fmt.Sprintln("Error: no stored trending videos"))
		}
		getTopVideosResponse = &pb.GetTopVideosResponse{
			Videos:        server.TrendingVideos.TrendingVideos[:int(math.Min(float64(req.Limit), float64(len(server.TrendingVideos.TrendingVideos))))],
			StaleResponse: true,
		}
	}

	if !wasError {
		wasStale = getTopVideosResponse.StaleResponse
		wasSuccess = true
	}
	return getTopVideosResponse, nil
}

func (server *VideoRecServiceServer) GoGetTopVideos(
	ctx context.Context,
	req *pb.GetTopVideosRequest,
) (*pb.GetTopVideosResponse, error) {
	userInfo, err := server.fetchUsers([]uint64{req.UserId})
	if err != nil {
		if server.options.DisableRetry {
			atomic.AddUint64(&server.UserServiceErrors, 1)
			return nil, retriveError("user", []uint64{req.UserId}, err)
		}
		userInfo, err = server.fetchUsers([]uint64{req.UserId})
		if err != nil {
			atomic.AddUint64(&server.UserServiceErrors, 1)
			return nil, retriveError("user", []uint64{req.UserId}, err)
		}
	}
	userCoef := userInfo[0].UserCoefficients

	SubscribedToUsersInfos, err := server.fetchUsersInBatches(userInfo[0].SubscribedTo, server.options.MaxBatchSize)
	if err != nil {
		errStatus, _ := status.FromError(err)
		return nil, status.Errorf(errStatus.Code(), errStatus.Message())
	}

	videosIds := getVideoIdsFromUsers(SubscribedToUsersInfos)

	VideosInfos, err := server.fetchVideosInBatches(videosIds, server.options.MaxBatchSize)
	if err != nil {
		errStatus, _ := status.FromError(err)
		return nil, status.Errorf(errStatus.Code(), errStatus.Message())
	}

	topVideos := RankVideosAndGetTop(userCoef, VideosInfos, req.Limit)

	return &pb.GetTopVideosResponse{Videos: topVideos}, nil
}

func (server *VideoRecServiceServer) getUserConnRoundRobin(conns []*grpc.ClientConn) *grpc.ClientConn {
	conn := conns[int(server.UserServiceConnIndex)%len(conns)]
	atomic.AddUint64(&server.UserServiceConnIndex, 1)
	return conn
}

func (server *VideoRecServiceServer) getVideoConnRoundRobin(conns []*grpc.ClientConn) *grpc.ClientConn {
	conn := conns[int(server.VideoServiceConnIndex)%len(conns)]
	atomic.AddUint64(&server.VideoServiceConnIndex, 1)
	return conn
}

func (server *VideoRecServiceServer) fetchUsers(userIds []uint64) ([]*upb.UserInfo, error) {
	userServiceConn := server.getUserConnRoundRobin(server.UserServiceConn)
	userServiceClient := upb.NewUserServiceClient(userServiceConn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out, err := userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: userIds})
	if err != nil {
		if server.options.DisableRetry {
			return nil, retriveError("user", userIds, err)
		}
		out, err = userServiceClient.GetUser(ctx, &upb.GetUserRequest{UserIds: userIds})
		if err != nil {
			return nil, retriveError("user", userIds, err)
		}
	}

	if len(out.Users) != len(userIds) {
		errStatus, _ := status.FromError(err)
		return nil, status.Errorf(errStatus.Code(), fmt.Sprintf("Length not consists: %d, %d", len(out.Users), len(userIds)))
	}
	return out.Users, nil
}

func (server *VideoRecServiceServer) fetchVideos(videoIds []uint64) ([]*vpb.VideoInfo, error) {
	videoServiceConn := server.getVideoConnRoundRobin(server.VideoServiceConn)
	videoServiceClient := vpb.NewVideoServiceClient(videoServiceConn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out, err := videoServiceClient.GetVideo(ctx, &vpb.GetVideoRequest{VideoIds: videoIds})
	if err != nil {
		if server.options.DisableRetry {
			return nil, retriveError("video", videoIds, err)
		}
		out, err = videoServiceClient.GetVideo(ctx, &vpb.GetVideoRequest{VideoIds: videoIds})
		if err != nil {
			return nil, retriveError("video", videoIds, err)
		}
	}

	if len(out.Videos) != len(videoIds) {
		errStatus, _ := status.FromError(err)
		return nil, status.Errorf(errStatus.Code(), fmt.Sprintf("Length not consists: %d, %d", len(out.Videos), len(videoIds)))
	}
	return out.Videos, nil
}

func (server *VideoRecServiceServer) fetchUsersInBatches(userIds []uint64, batchSize int) ([]*upb.UserInfo, error) {
	wasError := false
	wasSuccess := false
	defer func() {
		if !wasSuccess {
			atomic.AddUint64(&server.UserServiceErrors, 1)
		}
	}()

	var res []*upb.UserInfo
	for i := 0; i < len(userIds); i += server.options.MaxBatchSize {
		j := i + server.options.MaxBatchSize
		if j > len(userIds) {
			j = len(userIds)
		}
		out, err := server.fetchUsers(userIds[i:j])
		if err != nil {
			wasError = true
			return nil, retriveError("user", userIds[i:j], err)
		}
		res = append(res, out...)
	}

	if !wasError {
		wasSuccess = true
	}
	return res, nil
}

func (server *VideoRecServiceServer) fetchVideosInBatches(videoIds []uint64, batchSize int) ([]*vpb.VideoInfo, error) {
	wasError := false
	wasSuccess := false
	defer func() {
		if !wasSuccess {
			atomic.AddUint64(&server.VideoServiceErrors, 1)
		}
	}()

	var res []*vpb.VideoInfo
	for i := 0; i < len(videoIds); i += server.options.MaxBatchSize {
		j := i + server.options.MaxBatchSize
		if j > len(videoIds) {
			j = len(videoIds)
		}
		out, err := server.fetchVideos(videoIds[i:j])
		if err != nil {
			wasError = true
			return nil, retriveError("video", videoIds[i:j], err)
		}
		res = append(res, out...)
	}

	if !wasError {
		wasSuccess = true
	}
	return res, nil
}

func getVideoIdsFromUsers(UserInfos []*upb.UserInfo) []uint64 {
	var videosIds []uint64
	visitedVideoId := make(map[int]bool)
	for _, user := range UserInfos {
		for _, videoId := range user.LikedVideos {
			if _, value := visitedVideoId[int(videoId)]; !value {
				videosIds = append(videosIds, uint64(videoId))
				visitedVideoId[int(videoId)] = true
			}
		}
	}
	return videosIds
}

type videoToScore struct {
	video *vpb.VideoInfo
	score uint64
}

func RankVideosAndGetTop(userCoef *upb.UserCoefficients, VideosInfos []*vpb.VideoInfo, limit int32) []*vpb.VideoInfo {
	myRanker := ranker.BcryptRanker{}
	var videosToScores []videoToScore
	for _, video := range VideosInfos {
		videosToScores = append(videosToScores, videoToScore{
			video: video,
			score: myRanker.Rank(userCoef, video.VideoCoefficients)})
	}
	sort.Slice(videosToScores, func(i, j int) bool {
		return videosToScores[i].score > videosToScores[j].score
	})

	var topVideos []*vpb.VideoInfo
	count := int32(0)
	for _, videoToScore := range videosToScores {
		if count++; limit == 0 || count <= limit {
			topVideos = append(topVideos, videoToScore.video)
		}
	}
	return topVideos
}

func serviceConn(address string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return grpc.Dial(address, opts...)
}

func retriveError(s string, ids []uint64, err error) error {
	errStatus, _ := status.FromError(err)
	log.Println(errStatus.Code(), errStatus.Message())
	return status.Errorf(errStatus.Code(), fmt.Sprintf("Error retrieving %s information (Ids): %v\n", s, ids), err)
}

func (server *VideoRecServiceServer) GetStats(
	ctx context.Context,
	req *pb.GetStatsRequest,
) (*pb.GetStatsResponse, error) {
	return &pb.GetStatsResponse{
		TotalRequests:      server.TotalRequests,
		TotalErrors:        server.TotalErrors,
		ActiveRequests:     server.TotalRequests - server.TotalResponses,
		UserServiceErrors:  server.UserServiceErrors,
		VideoServiceErrors: server.VideoServiceErrors,
		AverageLatencyMs:   float32(server.TotalLatencyMs) / float32(server.TotalRequests),
		P99LatencyMs: float32(func() float64 {
			server.LatencyMss.mu.Lock()
			defer server.LatencyMss.mu.Unlock()
			return server.LatencyMss.td.Quantile(0.99)
		}()),
		StaleResponses: server.StaleResponses,
	}, nil
}

func (server *VideoRecServiceServer) GetTrendingVideos() (uint64, error) {
	videoServiceConn := server.getVideoConnRoundRobin(server.VideoServiceConn)
	videoServiceClient := vpb.NewVideoServiceClient(videoServiceConn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out, err := videoServiceClient.GetTrendingVideos(ctx, &vpb.GetTrendingVideosRequest{})
	if err != nil {
		return 0, retriveError("video", []uint64{}, err)
	}

	videosIds := out.Videos
	var VideosInfos []*vpb.VideoInfo
	for i := 0; i < len(videosIds); i += server.options.MaxBatchSize {
		j := i + server.options.MaxBatchSize
		if j > len(videosIds) {
			j = len(videosIds)
		}
		out, err := server.fetchVideos(videosIds[i:j])
		if err != nil {
			return 0, retriveError("video", videosIds[i:j], err)
		}
		VideosInfos = append(VideosInfos, out...)
	}

	server.TrendingVideos.mu.Lock()
	defer server.TrendingVideos.mu.Unlock()
	server.TrendingVideos.TrendingVideos = VideosInfos
	server.TrendingVideos.ExpirationTimeS = out.ExpirationTimeS
	return out.ExpirationTimeS, nil
}

func (server *VideoRecServiceServer) PeriodicallyGetTrendingVideos() {
	for {
		ExpirationTimeS, err := server.GetTrendingVideos()
		if err != nil {
			time.Sleep(10 * time.Second)
		} else {
			time.Sleep(time.Until(time.Unix(int64(ExpirationTimeS), 0)))
		}
	}
}
