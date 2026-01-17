using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using MareSynchronosShared.Services;
using MareSynchronosShared.Utils.Configuration;
using System.Collections.Concurrent;

namespace MareSynchronosStaticFilesServer.Services;

public sealed class ScalewayStorageService : IHostedService, IDisposable
{
    private readonly IConfigurationService<StaticFilesServerConfiguration> _config;
    private readonly ILogger<ScalewayStorageService> _logger;
    private readonly ConcurrentQueue<(string Hash, string FilePath)> _uploadQueue = new();
    private readonly SemaphoreSlim _uploadSemaphore = new(3);
    private readonly CancellationTokenSource _cts = new();
    private IAmazonS3? _s3Client;
    private Task? _processingTask;
    private bool _disposed;

    public bool IsEnabled => _config.GetValueOrDefault(nameof(StaticFilesServerConfiguration.ScalewayEnabled), false);

    public ScalewayStorageService(
        IConfigurationService<StaticFilesServerConfiguration> config,
        ILogger<ScalewayStorageService> logger)
    {
        _config = config;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!IsEnabled)
        {
            _logger.LogInformation("Scaleway storage is disabled");
            return Task.CompletedTask;
        }

        InitializeS3Client();
        _processingTask = ProcessUploadQueueAsync(_cts.Token);
        _logger.LogInformation("Scaleway storage service started");
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!IsEnabled) return;

        _cts.Cancel();

        if (_processingTask != null)
        {
            try
            {
                await _processingTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        _logger.LogInformation("Scaleway storage service stopped with {Count} items remaining in queue", _uploadQueue.Count);
    }

    private void InitializeS3Client()
    {
        var accessKey = _config.GetValue<string>(nameof(StaticFilesServerConfiguration.ScalewayAccessKey));
        var secretKey = _config.GetValue<string>(nameof(StaticFilesServerConfiguration.ScalewaySecretKey));
        var endpoint = _config.GetValue<string>(nameof(StaticFilesServerConfiguration.ScalewayEndpoint));
        var region = _config.GetValue<string>(nameof(StaticFilesServerConfiguration.ScalewayRegion));

        var s3Config = new AmazonS3Config
        {
            ServiceURL = endpoint,
            ForcePathStyle = true,
            AuthenticationRegion = region
        };

        _s3Client = new AmazonS3Client(accessKey, secretKey, s3Config);
    }

    public void QueueUpload(string hash, string filePath)
    {
        if (!IsEnabled || _s3Client == null) return;

        _uploadQueue.Enqueue((hash, filePath));
        _logger.LogDebug("Queued file {Hash} for Scaleway upload, queue size: {Size}", hash, _uploadQueue.Count);
    }

    public async Task<bool> FileExistsAsync(string hash, CancellationToken ct = default)
    {
        if (!IsEnabled || _s3Client == null) return false;

        var bucketName = _config.GetValue<string>(nameof(StaticFilesServerConfiguration.ScalewayBucketName));
        var key = GetS3Key(hash);

        try
        {
            await _s3Client.GetObjectMetadataAsync(bucketName, key, ct).ConfigureAwait(false);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    private async Task ProcessUploadQueueAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (_uploadQueue.TryDequeue(out var item))
                {
                    await _uploadSemaphore.WaitAsync(ct).ConfigureAwait(false);
                    _ = UploadFileAsync(item.Hash, item.FilePath, ct)
                        .ContinueWith(_ => _uploadSemaphore.Release(), TaskScheduler.Default);
                }
                else
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in upload queue processing");
                await Task.Delay(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false);
            }
        }
    }

    private async Task UploadFileAsync(string hash, string filePath, CancellationToken ct)
    {
        if (_s3Client == null) return;

        var bucketName = _config.GetValue<string>(nameof(StaticFilesServerConfiguration.ScalewayBucketName));
        var key = GetS3Key(hash);

        try
        {
            if (!File.Exists(filePath))
            {
                _logger.LogWarning("File {FilePath} no longer exists, skipping upload", filePath);
                return;
            }

            using var transferUtility = new TransferUtility(_s3Client);
            var uploadRequest = new TransferUtilityUploadRequest
            {
                FilePath = filePath,
                BucketName = bucketName,
                Key = key,
                ContentType = "application/octet-stream",
                StorageClass = S3StorageClass.Standard,
                CannedACL = S3CannedACL.PublicRead
            };

            await transferUtility.UploadAsync(uploadRequest, ct).ConfigureAwait(false);
            _logger.LogInformation("Successfully uploaded {Hash} to Scaleway", hash);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to upload {Hash} to Scaleway, will retry on next server restart", hash);
            _uploadQueue.Enqueue((hash, filePath));
        }
    }

    private static string GetS3Key(string hash)
    {
        return $"{hash[0]}/{hash}";
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts.Cancel();
        _cts.Dispose();
        _uploadSemaphore.Dispose();
        _s3Client?.Dispose();
    }
}
