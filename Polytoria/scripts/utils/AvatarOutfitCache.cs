// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

using Polytoria.Schemas.API;
using Polytoria.Shared;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Polytoria.Utils;

public struct AvatarFetchResult
{
	public bool NotModified { get; set; }
	public APIAvatarResponse? Avatar { get; set; }

	public static AvatarFetchResult Modified(APIAvatarResponse avatar) => new() { NotModified = false, Avatar = avatar };
	public static AvatarFetchResult Unchanged() => new() { NotModified = true };
}

public static class AvatarOutfitCache
{
	public static TimeSpan Ttl { get; set; } = TimeSpan.FromMinutes(5);
	public static int MaxEntries { get; set; } = 512;
	public static bool ServeStaleOnError { get; set; } = true;

	// Test seams (null => defaults). Resolved lazily so PolyAPI's static ctor isn't triggered in tests.
	internal static Func<int, Task<APIAvatarResponse>>? Fetcher;
	internal static Func<int, string?, Task<AvatarFetchResult>>? ConditionalFetcher;
	internal static Func<long>? NowMs;

	private sealed class Entry
	{
		public required Task<APIAvatarResponse> Task;
		public long CreatedAtMs;
		public APIAvatarResponse? LastGood;
	}

	private static readonly ConcurrentDictionary<int, Entry> _entries = new();
	private static long _hits;
	private static long _misses;

	public static int Count => _entries.Count;
	public static long Hits => Interlocked.Read(ref _hits);
	public static long Misses => Interlocked.Read(ref _misses);

	public static Task<APIAvatarResponse> GetAsync(int userID, bool forceRefresh = false, bool revalidate = false)
	{
		long now = (NowMs ?? (() => Environment.TickCount64))();
		long ttlMs = (long)Ttl.TotalMilliseconds;

		if (!forceRefresh && !revalidate && _entries.TryGetValue(userID, out Entry? existing))
		{
			if (!existing.Task.IsCompleted)
			{
				Interlocked.Increment(ref _hits);
				return existing.Task;
			}

			if (existing.Task.IsCompletedSuccessfully && (now - existing.CreatedAtMs < ttlMs))
			{
				Interlocked.Increment(ref _hits);
				return existing.Task;
			}
		}

		Interlocked.Increment(ref _misses);

		APIAvatarResponse? prevGood = null;
		if (_entries.TryGetValue(userID, out Entry? prior))
		{
			prevGood = prior.LastGood;
		}

		TaskCompletionSource<APIAvatarResponse> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

		Entry newEntry = new()
		{
			Task = tcs.Task,
			CreatedAtMs = now,
			LastGood = prevGood
		};

		_entries[userID] = newEntry;
		Prune();

		_ = FetchAndStoreAsync(userID, prevGood, newEntry, tcs, forceRefresh);
		return tcs.Task;
	}

	private static async Task FetchAndStoreAsync(int userID, APIAvatarResponse? prevGood, Entry entry, TaskCompletionSource<APIAvatarResponse> tcs, bool forceRefresh)
	{
		Func<int, string?, Task<AvatarFetchResult>> fetcher;
		if (ConditionalFetcher != null)
		{
			fetcher = ConditionalFetcher;
		}
		else if (Fetcher != null)
		{
			fetcher = async (id, etag) => AvatarFetchResult.Modified(await Fetcher(id).ConfigureAwait(false));
		}
		else
		{
			fetcher = PolyAPI.GetUserAvatarConditionalAsync;
		}

		string? etag = forceRefresh ? null : prevGood?.ID;

		try
		{
			AvatarFetchResult fetchResult = await fetcher(userID, etag).ConfigureAwait(false);
			long finishTime = (NowMs ?? (() => Environment.TickCount64))();

			if (fetchResult.NotModified && prevGood.HasValue)
			{
				entry.LastGood = prevGood.Value;
				entry.CreatedAtMs = finishTime;
				tcs.TrySetResult(prevGood.Value);
				return;
			}

			if (fetchResult.Avatar.HasValue)
			{
				APIAvatarResponse result = fetchResult.Avatar.Value;
				entry.LastGood = result;
				entry.CreatedAtMs = finishTime;
				tcs.TrySetResult(result);
				return;
			}

			throw new HttpRequestException("Avatar response was empty and not 304 Not Modified");
		}
		catch (Exception ex)
		{
			if (ServeStaleOnError && prevGood.HasValue)
			{
				PT.PrintErr($"Failed to refresh avatar for user {userID}, serving stale outfit: {ex.Message}");
				entry.LastGood = prevGood;
				entry.CreatedAtMs = 0;
				tcs.TrySetResult(prevGood.Value);
				return;
			}

			_entries.TryRemove(new KeyValuePair<int, Entry>(userID, entry));
			tcs.TrySetException(ex);
		}
	}

	private static void Prune()
	{
		if (_entries.Count <= MaxEntries) return;

		long now = (NowMs ?? (() => Environment.TickCount64))();
		long ttlMs = (long)Ttl.TotalMilliseconds;

		foreach (var kvp in _entries)
		{
			if (_entries.Count <= MaxEntries) return;
			if (kvp.Value.Task.IsCompleted && (now - kvp.Value.CreatedAtMs >= ttlMs))
			{
				_entries.TryRemove(kvp);
			}
		}

		if (_entries.Count > MaxEntries)
		{
			var candidates = _entries
				.Where(kvp => kvp.Value.Task.IsCompleted)
				.OrderBy(kvp => kvp.Value.CreatedAtMs)
				.ToList();

			foreach (var kvp in candidates)
			{
				if (_entries.Count <= MaxEntries) break;
				_entries.TryRemove(kvp);
			}
		}
	}

	public static void Invalidate(int userID)
	{
		_entries.TryRemove(userID, out _);
	}

	public static void Clear()
	{
		_entries.Clear();
		Interlocked.Exchange(ref _hits, 0);
		Interlocked.Exchange(ref _misses, 0);
	}
}

