using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using System.Web;
using System.Web.Caching;

namespace CircuitBreaker
{
    public class CircuitBreaker
    {
        private readonly object _circuitBreakerLock = new object();

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
                        _cache = Cache;
                }

                return _cache;
            }
        }
        private CircuitBreakerState state;
        private Exception lastExecutionException = null;

        public int Failures { get; private set; }
        public int Threshold { get; private set; }
        public TimeSpan OpenTimeout { get; private set; }

        public string CacheKey { get; private set; }
        public CacheDependency CacheDependency { get; private set; }
        public TimeSpan CacheDuration { get; private set; }

        public bool FileBacked { get; private set; }
        public string FileName { get; private set; }
        public string WorkingDirectory { get; private set; }

        public CircuitBreaker(
            string cacheKey = null,
            CacheDependency cacheDependency = null,
            TimeSpan? cacheDuration = null,
            bool fileBacked = false,
            string workingDirectory = null,
            int failureThreshold = 3,
            TimeSpan? openTimeout = null
            )
        {
            if (cacheDuration == null)
                cacheDuration = TimeSpan.FromMinutes(5);

            if (cacheDuration == TimeSpan.Zero)
                throw new ArgumentNullException("cacheDuration", "You must specify a cache duration greater than 0");

            if (openTimeout == null)
                openTimeout = TimeSpan.FromSeconds(5);

            Threshold = failureThreshold;
            OpenTimeout = (TimeSpan)openTimeout;

            CacheKey = cacheKey;
            CacheDependency = cacheDependency;
            CacheDuration = (TimeSpan)cacheDuration;

            FileBacked = fileBacked;
            WorkingDirectory = workingDirectory ?? @"c:/Temp";

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
            T resp = default(T);
            this.lastExecutionException = null;

            #region Initiation Execution
            lock (_circuitBreakerLock)
            {
                state.ExecutionStart();
                if (state is OpenState)
                {
                    return resp; // Stop execution of this method
                }
            }
            #endregion

            #region Do the work
            try
            {
                //Access Without Cache
                if (String.IsNullOrWhiteSpace(FileName))
                {
                    //lock here, to protect volatile resource
                    lock (_circuitBreakerLock)
                    {
                        resp = funcToInvoke(); ;
                    }
                }
                else
                {
                    //check mem cache
                    resp = ReadFromCache<T>();

                    //mem cache is empty
                    if (resp == null)
                    {
                        if (FileBacked)
                        {
                            //check file system
                            resp = ReadFromFile<T>();

                            //file system also empty
                            if (resp == null)
                            {
                                //check cache one more time
                                resp = ReadFromCache<T>();

                                if (resp == null)
                                {
                                    //lock here, to protect volatile resource
                                    lock (_circuitBreakerLock)
                                    {
                                        resp = funcToInvoke();

                                        AddToCache(resp);
                                        WriteToFile(resp);
                                    }
                                }
                            }
                            else
                            {
                                //read from file system is "fresh", pump into mem cache
                                AddToCache(resp);
                            }
                        }
                        else
                        {
                            //check cache one more time
                            resp = ReadFromCache<T>();

                            if (resp == null)
                            {
                                //lock here, to protect volatile resource
                                lock (_circuitBreakerLock)
                                {
                                    resp = funcToInvoke();

                                    AddToCache(resp);
                                    WriteToFile(resp);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                lastExecutionException = e;
                lock (_circuitBreakerLock)
                {
                    state.ExecutionFail(e);
                }
                return resp; // Stop execution of this method
            }
            #endregion

            #region Cleanup Execution
            lock (_circuitBreakerLock)
            {
                state.ExecutionComplete();
            }
            #endregion

            return resp;
        }

        /// <summary>
        /// Executes a specified Action within the confines of the Circuit Breaker Pattern (https://msdn.microsoft.com/en-us/library/dn589784.aspx)
        /// </summary>
        /// <param name="actionToInvoke"></param>
        /// <returns>Circuit Breaker instance, allows client to determine situational handling (Closed/Half-Open/Open)</returns>
        public CircuitBreaker ExecuteAction(Action actionToInvoke)
        {
            this.lastExecutionException = null;

            #region Initiation Execution
            lock (_circuitBreakerLock)
            {
                state.ExecutionStart();
                if (state is OpenState)
                {
                    return this; // Stop execution of this method
                }
            }
            #endregion

            #region Do the work
            try
            {
                actionToInvoke();
            }
            catch (Exception e)
            {
                lastExecutionException = e;
                lock (_circuitBreakerLock)
                {
                    state.ExecutionFail(e);
                }
                return this; // Stop execution of this method
            }
            #endregion

            #region Cleanup Execution
            lock (_circuitBreakerLock)
            {
                state.ExecutionComplete();
            }
            #endregion

            return this;
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

        public void Close()
        {
            lock (_circuitBreakerLock)
            {
                MoveToClosedState();
            }
        }

        public void Open()
        {
            lock (_circuitBreakerLock)
            {
                MoveToOpenState();
            }
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
        internal T ReadFromCache<T>()
        {
            var res = Cache[CacheKey];
            if (res is T)
                return (T)res;
            else
                return default(T);
        }

        internal void AddToCache<T>(T obj)
        {
            T inCache = ReadFromCache<T>();

            if (inCache != null)
                Cache.Remove(CacheKey);

            Cache.Add(CacheKey, obj, CacheDependency, DateTime.Now.Add(CacheDuration), Cache.NoSlidingExpiration, CacheItemPriority.NotRemovable, null);
        }
        #endregion

        #region File Management
        internal T ReadFromFile<T>()
        {
            T resp = default(T);
            string path = ResolveFilePath();

            if (!string.IsNullOrWhiteSpace(path))
            {
                FileInfo fi = new FileInfo(path);

                if (fi.Exists)
                {
                    lock (_circuitBreakerLock)
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

                            if (resp is T)
                            {
                                //check if the objet is in mem cache, if not add it
                                AddToCache(resp);
                            }
                        }
                    }
                }
            }

            return resp;
        }

        internal void WriteToFile<T>(T objToWrite)
        {
            if (!FileBacked)
                return;

            string objStr = JsonConvert.SerializeObject(objToWrite);
            string path = ResolveFilePath();

            if (!string.IsNullOrWhiteSpace(path))
            {
                FileInfo fi = new FileInfo(path);

                lock (_circuitBreakerLock)
                {
                    if (fi.Exists)
                        fi.Delete();

                    File.WriteAllText(path, objStr);

                    //write to object cache
                    AddToCache(objToWrite);
                }
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
            string wd = @"c:/Temp";

            if (!Directory.Exists(wd))
                Directory.CreateDirectory(wd);

            string fileName = FileName;
            //check if there's a trailing slash, if not add
            if (wd.Substring(wd.Length - 1) != "/")
            {
                fileName = String.Format("/{0}", fileName);
            }

            return String.Format("{0}{1}", wd, fileName);
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
