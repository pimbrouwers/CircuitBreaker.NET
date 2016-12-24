using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web;
using System.Web.Caching;

namespace CircuitBreaker
{
    public class CircuitBreaker
    {
        private static ConcurrentDictionary<string, object> CircuitBreakerLocks = new ConcurrentDictionary<string, object>();
        private static ConcurrentDictionary<string, int> CircuitBreakerExceptionCounts = new ConcurrentDictionary<string, int>();

        public string CircuitId { get; set; }

        public object CircuitBreakLock
        {
            get
            {
                return CircuitBreakerLocks.GetOrAdd(CircuitId, new object());
            }
        }

        private Cache _cache;
        private Cache Cache
        {
            get
            {
                if (_cache == null)
                {
                    if (HttpContext.Current == null)
                        _cache = HttpRuntime.Cache;
                    else
                        _cache = HttpContext.Current.Cache;
                }

                return _cache;
            }
        }
        private CircuitBreakerState state;
        private Exception lastExecutionException = null;

        public int Failures
        {
            get
            {
                return CircuitBreakerExceptionCounts.GetOrAdd(CircuitId, 0);
            }
            set
            {
                CircuitBreakerExceptionCounts.AddOrUpdate(CircuitId, value, (key, oldValue) => value);
            }
        }
        public int Threshold { get; private set; }
        public TimeSpan OpenTimeout { get; private set; }

        public string CacheKey { get; private set; }
        public CacheDependency CacheDependency { get; private set; }
        public TimeSpan CacheDuration { get; private set; }
        public TimeSpan CacheSlidingExpiration { get; private set; }
        public CacheItemPriority CacheItemPriority { get; private set; }

        public bool FileBacked
        {
            get
            {
                return !string.IsNullOrWhiteSpace(WorkingDirectory);
            }
        }
        public string FileName { get; private set; }
        public string WorkingDirectory { get; private set; }

        public CircuitBreaker(
            string circuitId,
            string cacheKey = null,
            CacheDependency cacheDependency = null,
            TimeSpan? cacheDuration = null,
            TimeSpan? cacheSlidingExpiration = null,
            CacheItemPriority cacheItemPriority = CacheItemPriority.Normal,
            string workingDirectory = null,
            int failureThreshold = 3,
            TimeSpan? openTimeout = null
            )
        {
            if (cacheDuration == null)
                cacheDuration = TimeSpan.FromMinutes(5);

            if (cacheDuration == TimeSpan.Zero)
                throw new ArgumentNullException("cacheDuration", "You must specify a cache duration greater than 0");

            if (cacheSlidingExpiration == null)
                cacheSlidingExpiration = Cache.NoSlidingExpiration;

            if (openTimeout == null)
                openTimeout = TimeSpan.FromSeconds(5);

            CircuitId = circuitId;

            Threshold = failureThreshold;
            OpenTimeout = (TimeSpan)openTimeout;

            CacheKey = cacheKey;
            CacheDependency = cacheDependency;
            CacheDuration = (TimeSpan)cacheDuration;
            CacheSlidingExpiration = (TimeSpan)cacheSlidingExpiration;
            CacheItemPriority = cacheItemPriority;

            WorkingDirectory = workingDirectory;

            if (FileBacked)
            {
                FileName = GetFileNameFromCacheKey();
            }

            //Initialize
            MoveToClosedState();
        }


        /// <summary>
        /// Executes a specified Func<T> within the confines of the Circuit Breaker Pattern (https://msdn.microsoft.com/en-us/library/dn589784.aspx)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="funcToIvoke"></param>
        /// <returns>Object of type T of default(T)</returns>
        public T Execute<T>(Func<T> funcToInvoke)
        {
            object circuitBreakerLock = CircuitBreakLock;

            T resp = default(T);
            this.lastExecutionException = null;

            #region Initiation Execution
            lock (circuitBreakerLock)
            {
                state.ExecutionStart();
                if (state is OpenState)
                {
                    return resp; //Stop execution of this method
                }
            }
            #endregion

            #region Do the work
            try
            {
                //Access Without Cache
                if (String.IsNullOrWhiteSpace(FileName))
                {
                    lock (circuitBreakerLock)
                    {
                        //do the work
                        resp = funcToInvoke();
                    }
                }
                else
                {
                    //check mem cache
                    if (!ReadFromCache<T>(out resp))
                    {
                        lock (circuitBreakerLock)
                        {
                            if (!ReadFromCache<T>(out resp))
                            {
                                if (FileBacked)
                                {
                                    //check file system
                                    if (!ReadFromFile<T>(out resp))
                                    {
                                        //do the work
                                        resp = funcToInvoke();

                                        AddToCache(resp);
                                        WriteToFile(resp);
                                    }
                                    else
                                    {
                                        //read from file system is "fresh", pump into mem cache
                                        AddToCache(resp);
                                    }
                                }
                                else
                                {
                                    //do the work
                                    resp = funcToInvoke();

                                    AddToCache(resp);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                lock (circuitBreakerLock)
                {
                    lastExecutionException = e;
                    state.ExecutionFail(e);
                }

                throw;
            }
            finally
            {
                lock (circuitBreakerLock)
                {
                    state.ExecutionComplete();
                }
            }
            #endregion

            return resp;
        }

        /// <summary>
        /// Executes a specified Func<T> within the confines of the Circuit Breaker Pattern (https://msdn.microsoft.com/en-us/library/dn589784.aspx)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="funcToIvoke"></param>
        /// <returns>Object of type T of default(T)</returns>
        public async Task<T> ExecuteAsync<T>(Func<Task<T>> funcToInvoke)
        {
            T resp = default(T);
            this.lastExecutionException = null;

            #region Initiation Execution
            state.ExecutionStart();
            if (state is OpenState)
            {
                return resp; //Stop execution of this method
            }
            #endregion

            #region Do the work
            try
            {
                //Access Without Cache
                if (String.IsNullOrWhiteSpace(FileName))
                {
                    //do the work
                    resp = await funcToInvoke();
                }
                else
                {
                    //check mem cache
                    if (!ReadFromCache<T>(out resp))
                    {
                        if (!ReadFromCache<T>(out resp))
                        {
                            if (FileBacked)
                            {
                                //check file system
                                if (!ReadFromFile<T>(out resp))
                                {
                                    //do the work
                                    resp = await funcToInvoke();

                                    AddToCache(resp);
                                    WriteToFile(resp);
                                }
                                else
                                {
                                    //read from file system is "fresh", pump into mem cache
                                    AddToCache(resp);
                                }
                            }
                            else
                            {
                                //do the work
                                resp = await funcToInvoke();

                                AddToCache(resp);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                lastExecutionException = e;
                state.ExecutionFail(e);

                throw;
            }
            finally
            {
                state.ExecutionComplete();
            }
            #endregion

            return resp;
        }

        #region State Management
        public bool IsClosed
        {
            get { return state.Update() is ClosedState; }
        }

        public bool IsOpen
        {
            get { return state.Update() is OpenState; }
        }

        public bool IsHalfOpen
        {
            get { return state.Update() is HalfOpenState; }
        }

        public bool IsThresholdReached()
        {
            return Failures >= Threshold;
        }

        public Exception GetLastExecutionException()
        {
            return lastExecutionException;
        }

        void Close()
        {
            MoveToClosedState();
        }

        void Open()
        {
            MoveToOpenState();
        }

        internal CircuitBreakerState MoveToClosedState()
        {
            state = new ClosedState(this);
            return state;
        }

        internal CircuitBreakerState MoveToOpenState()
        {
            state = new OpenState(this);
            return state;
        }

        internal CircuitBreakerState MoveToHalfOpenState()
        {
            state = new HalfOpenState(this);
            return state;
        }

        internal void IncreaseFailureCount()
        {
            Failures++;
        }

        internal void ResetFailureCount()
        {
            Failures = 0;
        }
        #endregion

        #region Caching
        internal bool ReadFromCache<T>(out T resp)
        {
            bool success = true;
            var res = Cache[CacheKey];

            if (res is T)
                resp = (T)res;
            else
            {
                success = false;
                resp = default(T);
            }

            return success;
        }

        internal void AddToCache<T>(T obj)
        {
            if (obj != null)
            {
                Cache.Remove(CacheKey);
                Cache.Add(CacheKey, obj, CacheDependency, DateTime.Now.Add(CacheDuration), CacheSlidingExpiration, CacheItemPriority, null);
            }
        }
        #endregion

        #region File Management
        internal bool ReadFromFile<T>(out T resp)
        {
            bool success = false;
            resp = default(T);
            string path = ResolveFilePath();

            if (!string.IsNullOrWhiteSpace(path))
            {
                FileInfo fi = new FileInfo(path);

                if (fi.Exists)
                {

                    //if this file is "too old" delete
                    if (DateTime.UtcNow - fi.LastWriteTimeUtc > CacheDuration)
                    {
                        fi.Delete();
                    }
                    //file is still fresh enough, read and pump into mem cahce
                    else
                    {
                        string res = File.ReadAllText(path);
                        resp = JsonConvert.DeserializeObject<T>(res);
                        success = true;
                    }

                }
            }

            return success;
        }

        internal void WriteToFile<T>(T objToWrite)
        {
            if (!FileBacked)
                return;

            if (objToWrite == null)
                return;

            string objStr = JsonConvert.SerializeObject(objToWrite);
            string path = ResolveFilePath();

            if (!string.IsNullOrWhiteSpace(path))
            {
                FileInfo fi = new FileInfo(path);

                if (fi.Exists)
                    fi.Delete();

                File.WriteAllText(path, objStr);

                //write to object cache
                AddToCache(objToWrite);
            }

        }

        internal string SerializeObject<T>(T objToSerialize)
        {
            string objStr = null;

            if (objToSerialize is DataTable)
                objStr = JsonConvert.SerializeObject(objToSerialize, new DataTableConverter());
            else if (objToSerialize is DataSet)
                objStr = JsonConvert.SerializeObject(objToSerialize, new DataSetConverter());
            else
                objStr = JsonConvert.SerializeObject(objToSerialize);

            return objStr;
        }

        internal string GetFileNameFromCacheKey()
        {
            if (string.IsNullOrEmpty(CacheKey)) throw new ArgumentNullException("CacheKey", "A cache key must be specified");

            string str = CacheKey.ToLower();

            str = Regex.Replace(str, @"&\w+;", "");
            str = Regex.Replace(str, @"[^a-z0-9\-\s]", "", RegexOptions.IgnoreCase);
            str = str.Replace(" ", "-");
            str = Regex.Replace(str, @"-{2,}", "-");

            return str;
        }

        internal string ResolveFilePath()
        {
            if (!Directory.Exists(WorkingDirectory))
                Directory.CreateDirectory(WorkingDirectory);

            string fileName = FileName;
            //check if there's a trailing slash, if not add
            if (WorkingDirectory.Substring(WorkingDirectory.Length - 1) != "/")
            {
                fileName = String.Format("/{0}", fileName);
            }

            return String.Format("{0}{1}", WorkingDirectory, fileName);
        }
        #endregion
    }

    public abstract class CircuitBreakerState
    {
        protected readonly CircuitBreaker circuitBreaker;

        protected CircuitBreakerState(CircuitBreaker circuitBreaker)
        {
            this.circuitBreaker = circuitBreaker;
        }

        public virtual CircuitBreaker ExecutionStart()
        {
            return this.circuitBreaker;
        }
        public virtual void ExecutionComplete() { }
        public virtual void ExecutionFail(Exception e) { circuitBreaker.IncreaseFailureCount(); }

        public virtual CircuitBreakerState Update()
        {
            return this;
        }
    }

    public class OpenState : CircuitBreakerState
    {
        private readonly DateTime openDateTime; //last time something went wrong, or breaker was initialized
        public OpenState(CircuitBreaker circuitBreaker)
            : base(circuitBreaker)
        {
            //initialize openDateTime
            openDateTime = DateTime.UtcNow;
        }

        public override CircuitBreaker ExecutionStart()
        {
            //kickoff execution
            base.ExecutionStart();
            this.Update();
            return base.circuitBreaker;
        }

        public override CircuitBreakerState Update()
        {
            base.Update();

            if (DateTime.UtcNow >= openDateTime + base.circuitBreaker.OpenTimeout)
            {
                //timeout has passed, progress state to "half-open"
                return circuitBreaker.MoveToHalfOpenState();
            }

            return this;
        }
    }

    public class HalfOpenState : CircuitBreakerState
    {
        public HalfOpenState(CircuitBreaker circuitBreaker) : base(circuitBreaker) { }

        public override void ExecutionFail(Exception e)
        {
            //FAIL, set back to "open"
            base.ExecutionFail(e);
            circuitBreaker.MoveToOpenState();
        }

        public override void ExecutionComplete()
        {
            //SUCCESS, set to "closed"
            base.ExecutionComplete();
            circuitBreaker.MoveToClosedState();
        }
    }

    public class ClosedState : CircuitBreakerState
    {
        public ClosedState(CircuitBreaker circuitBreaker)
            : base(circuitBreaker)
        {
            //Reset fail count as soon as we have a success
            circuitBreaker.ResetFailureCount();
        }

        public override void ExecutionFail(Exception e)
        {
            base.ExecutionFail(e);
            if (circuitBreaker.IsThresholdReached())
            {
                //if we've reached the specified fail threshold, set to "open state"
                circuitBreaker.MoveToOpenState();
            }
        }
    }
}
