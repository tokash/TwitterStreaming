using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Streaminvi;
using TwitterToken;
using TweetinCore;
using TweetinCore.Interfaces.TwitterToken;
using TweetinCore.Interfaces;
using System.Timers;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Configuration;
using System.Data;
using System.Collections.Specialized;
using Tweetinvi;

namespace TwitterStreaming
{
    class TwitterStreamer
    {
        string ConsumerKey1 = System.Configuration.ConfigurationManager.AppSettings["ConsumerKey1"];
        string ConsumerSecret1 = System.Configuration.ConfigurationManager.AppSettings["ConsumerSecret1"];
        string AccessToken1 = System.Configuration.ConfigurationManager.AppSettings["AccessToken1"];
        string AccessTokenSecret1 = System.Configuration.ConfigurationManager.AppSettings["AccessTokenSecret1"];

        private static readonly string DBNamePath = System.Configuration.ConfigurationManager.AppSettings["DBNamePath"];
        private static readonly string dbName = System.Configuration.ConfigurationManager.AppSettings["DBName"];
        private static readonly string sqlserverName = System.Configuration.ConfigurationManager.AppSettings["SQLServerName"];

        internal static readonly string connStringInitial = "Server=" + Environment.MachineName + "\\SQLEXPRESS;User Id=sa;Password=tokash30;database=master";
        internal static readonly string connString = "Server=" + Environment.MachineName + "\\SQLEXPRESS;User Id=sa;Password=tokash30;database=" + dbName;
        
        public static string TweetsTableSchema = @"(TweetID nvarchar (25) PRIMARY KEY, UserID nvarchar (25), Tweet nvarchar (4000), TimeOfTweet nvarchar (40))";
        internal static readonly string[] TweetsTableColumns = { "TweetID", "UserID", "Tweet", "TimeOfTweet"};

        public static string ReTweetsTableSchema = @"(ReTweetID nvarchar (25) PRIMARY KEY, UserID nvarchar (25), SourceTweetID nvarchar (25), SourceUserID nvarchar (25), TimeOfReTweet nvarchar (40))";
        internal static readonly string[] RetweetsTableColumns = { "ReTweetID", "UserID", "SourceTweetID", "SourceUserID", "TimeOfReTweet" };

        public static string TweetsHashtagsTableSchema = @"(TweetID nvarchar (25), Hashtag nvarchar (140))";
        internal static readonly string[] TweetsHashtagsTableColumns = { "TweetID", "Hashtag" };

        private static readonly string[] tableNames = { "Tweets", "Retweets", "TweetsHashtags"};
        private static readonly string[] tableSchemas = { "CREATE TABLE Tweets (TweetID nvarchar (25) PRIMARY KEY, UserID nvarchar (25), Tweet nvarchar (4000), TimeOfTweet nvarchar (40))",
                                                          "CREATE TABLE Retweets (ReTweetID nvarchar (25) PRIMARY KEY, UserID nvarchar (25), SourceTweetID nvarchar (25), SourceUserID nvarchar (25), TimeOfReTweet nvarchar (40))",
                                                          "CREATE TABLE TweetsHashtags (TweetID nvarchar (25), Hashtag nvarchar (140))"};

        private static readonly string sqlCommandCreateDB = "CREATE DATABASE " + dbName + " ON PRIMARY " +
                "(NAME = " + dbName + ", " +
                "FILENAME = '" + DBNamePath + dbName + ".mdf', " +
                "SIZE = 3MB, MAXSIZE = 10MB, FILEGROWTH = 10%) " +
                "LOG ON (NAME = " + dbName + "_LOG, " +
                "FILENAME = '" + DBNamePath + dbName + ".ldf', " +
                "SIZE = 1MB, " +
                "MAXSIZE = 100MB, " +
                "FILEGROWTH = 10%)";

        #region Members
        SimpleStream _SimpleStream;
        FilteredStream _FilteredStream;

        List<TweetinCore.Interfaces.ITweet> _Tweets = new List<TweetinCore.Interfaces.ITweet>();
        //UserStream _UserStream;
        IToken _Token;
        Timer _Timer = new Timer();
        bool _IsStreamStopped = false;
        Action<TweetinCore.Interfaces.ITweet, string> _FileDataHandlerMethod;
        Action<TweetinCore.Interfaces.ITweet, int> _DataHandlerMethod;
        int _NumberOfTweetsToGet = 0;
        List<string> _Keywords = new List<string>();
        List<string> _Hashtags = new List<string>();
        NameValueCollection _ApplicationSettings = new NameValueCollection();
        #endregion

        /// <summary>
        /// C'tor
        /// </summary>
        /// <param name="iUrl">The URL to stream from</param>
        /// <param name="iPeriod">The period of time in miliseconds in which streaming will occur</param>
        public TwitterStreamer(string iUrl, double iPeriod )
        {
            _Token = new Token(AccessToken1, AccessTokenSecret1, ConsumerKey1, ConsumerSecret1);
            _SimpleStream = new SimpleStream(iUrl);
            _SimpleStream.StreamStopped += new EventHandler<TweetinCore.Events.GenericEventArgs<Exception>>(OnStreamStopped);
            
            //_FilteredStream = new FilteredStream();

            //_FilteredStream.AddTrack("#android");
            //_UserStream = new UserStream();

            _NumberOfTweetsToGet = int.Parse(ConfigurationManager.AppSettings["NumberOfTweetsToGet"]);

            _Timer.Interval = iPeriod;
            _Timer.AutoReset = true; //Stops it from repeating
            _Timer.Start();
            _Timer.Elapsed += new ElapsedEventHandler(TimerElapsed);

            ReadConfigurationSection("Keywords", ref _Keywords, true);
            ReadConfigurationSection("Hashtags", ref _Hashtags, false);

            CreateEmptyDB();
        }

        private void ReadConfigurationSection(string iConfigurationSection, ref List<string> oContainer, bool iMakeLowercase)
        {
            NameValueCollection temp = (NameValueCollection)ConfigurationManager.GetSection(iConfigurationSection);

            foreach (string key in temp)
            {

                if (iMakeLowercase)
                {
                    oContainer.Add(temp[key].ToLower());
                }
                else
                {
                    oContainer.Add(temp[key]);
                }
            }
        }

        private void ReadConfigurationSection(string iConfigurationSection, ref NameValueCollection oContainer)
        {
            oContainer = (NameValueCollection)ConfigurationManager.GetSection(iConfigurationSection);
        }

        private void OnStreamStopped(object sender, TweetinCore.Events.GenericEventArgs<Exception> e)
        {
            _IsStreamStopped = true;
            if (e.Value != null)
            {
                Console.WriteLine(e.Value.ToString());
            }
        }

        public void GetStreamIntoFile()
        {
            string filename = "Stream_" + DateTime.Now.ToString("dd.MM.yyyy.HH.mm.ss.ffff");
            _FileDataHandlerMethod = HandleTweet;

            _SimpleStream.StartStream(_Token, x => _FileDataHandlerMethod(x, filename));
            //_FilteredStream.StartStream(_Token, x => _DataHandlerMethod(x, filename));
        }

        private bool IstMatch(string iMessage, string iSearchPattern)
        {
            bool retVal = false;

            
            if (Regex.IsMatch(iMessage, iSearchPattern))
            {
                retVal = true;
            }           

            return retVal;
        }

        public void GetTweets()
        {
            //_DataHandlerMethod = GetTweetsHandler;
            Stopwatch swTimer = new Stopwatch();

            swTimer.Start();
            Console.WriteLine(string.Format("{0}: Started looking for tweets...", DateTime.Now));


            while (_Tweets.Count < _NumberOfTweetsToGet)
            {
                _SimpleStream.StartStream(_Token, tweet =>
                {
                    
                    if (!Regex.IsMatch(tweet.Text, "[^\u0000-\u0080]+")) //get latin tweets only
                    {
                        if (IsContainKeywords(tweet.Text) || IsContainsHashtag(tweet.Hashtags))
                        {
                            //_Tweets.Add(tweet.Retweeting);
                            _Tweets.Add(tweet);

                            if (tweet.Retweeting != null)
                            {
                                _Tweets.Add(tweet.Retweeting);
                            }
                        }
                    } 
                });

                _Timer.Start();
            }
            
            Console.WriteLine("Finished looking for tweets...");
            swTimer.Stop();
            Console.WriteLine(String.Format("{0}: Looking for tweets took: {1}", DateTime.Now, swTimer.Elapsed));

            int i = 0;
            while (i < _Tweets.Count)
            {

                if (_Tweets[i].RetweetCount > 0)
                {
                    try
                    {
                        Console.WriteLine(String.Format("{0}: Started getting retweets for tweet:{1} ...", DateTime.Now, _Tweets[i].IdStr));
                        _Tweets[i].Retweets = _Tweets[i].GetRetweets(false, false, _Token);
                        Console.WriteLine(String.Format("{0}: Finished getting retweets for tweet:{1}...", DateTime.Now, _Tweets[i].IdStr));
                    }
                    catch (Exception ex)
                    {
                        //reached limit
                        if (ex.Message.Contains("429"))
                        {
                            Console.WriteLine(string.Format("{0}: Rate limit reached for: {1}, waiting 15 minutes...", DateTime.Now, _Tweets[i].IdStr));
                            System.Threading.Thread.Sleep(900000);
                        }
                        else
                        {
                            Console.WriteLine(ex.ToString());
                            i++;
                        }
                    }
                }
                else
                {
                    Console.WriteLine(string.Format("Tweet {0} as no retweets.", _Tweets[i].IdStr));
                }

                Console.WriteLine(string.Format("Adding tweet {0} to DB...", _Tweets[i].IdStr));
                AddTweetToDB(_Tweets[i]);
                Console.WriteLine(string.Format("Tweet {0} added to DB...", _Tweets[i].IdStr));
                Console.WriteLine(String.Format("Tweets to go: {0}", _Tweets.Count - i));
                i++;
            }            

        }

        private bool IsContainKeywords(string iTweetMessage)
        {
            bool isFound = false;

            foreach (string word in _Keywords)
            {
                string wordToLower = word.ToLower();
                if (IstMatch(iTweetMessage, string.Format(" (?i){0} ", wordToLower)))
                {
                    isFound = true;
                    break;
                }
            }

            return isFound;
        }

        private bool IsContainsHashtag(List<IHashTagEntity> iHashtags)
        {
            bool isFound = false;

            if (iHashtags != null && iHashtags.Count > 0)
            {
                foreach (IHashTagEntity hashtag in iHashtags)
                {
                    if (_Hashtags.Contains(hashtag.Text))
                    {
                        isFound = true;
                    }
                }
            }

            return isFound;
        }

        public void PrintTweets()
        {
            foreach (ITweet tweet in _Tweets)
            {
                string s = string.Format("Twitter: {0}\nMessage: {1}\nDate: {2}\n", tweet.Creator.Name, tweet.Text, tweet.CreatedAt);
                Console.WriteLine(s);
            }
        }

        private void AddTweetToDB(ITweet iTweet)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            parameters.Add(String.Format("@{0}", TweetsTableColumns[0]), iTweet.IdStr);
            parameters.Add(String.Format("@{0}", TweetsTableColumns[1]), iTweet.Creator.IdStr);
            parameters.Add(String.Format("@{0}", TweetsTableColumns[2]), iTweet.Text.Replace("\n", ""));
            parameters.Add(String.Format("@{0}", TweetsTableColumns[3]), iTweet.CreatedAt.ToString());
            
            try
            {
                string sqlCmd = string.Format("select TweetID from tweets where TweetID='{0}'", iTweet.IdStr);
                DataTable result = SQLServerCommon.SQLServerCommon.ExecuteQuery(sqlCmd, connString);
                if (result.Rows.Count == 0)
                {
                    SQLServerCommon.SQLServerCommon.Insert("Tweets", connString, TweetsTableColumns, parameters); 
                }
            }
            catch (Exception)
            {

                throw;
            }

            if (iTweet.Hashtags != null)
            {
                foreach (IHashTagEntity hashtag in iTweet.Hashtags)
                {
                    AddHashTagToDB(iTweet.IdStr, hashtag.Text);
                }
            }

            if (iTweet.Retweets != null)
            {
                foreach (ITweet retweet in iTweet.Retweets)
                {
                    AddReTweetToDB(retweet, iTweet);
                }
            }
        }

        private void AddReTweetToDB(ITweet iReTweet, ITweet iTweet)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            parameters.Add(String.Format("@{0}", RetweetsTableColumns[0]), iReTweet.IdStr);
            parameters.Add(String.Format("@{0}", RetweetsTableColumns[1]), iReTweet.Creator.IdStr);
            parameters.Add(String.Format("@{0}", RetweetsTableColumns[2]), iTweet.IdStr);
            parameters.Add(String.Format("@{0}", RetweetsTableColumns[3]), iTweet.Creator.IdStr);
            parameters.Add(String.Format("@{0}", RetweetsTableColumns[4]), iReTweet.CreatedAt.ToString());


            try
            {
                string sqlCmd = string.Format("select ReTweetID from Retweets where ReTweetID='{0}'", iReTweet.IdStr);
                DataTable result = SQLServerCommon.SQLServerCommon.ExecuteQuery(sqlCmd, connString);
                if (result.Rows.Count == 0)
                {
                    SQLServerCommon.SQLServerCommon.Insert("Retweets", connString, RetweetsTableColumns, parameters);
                }
            }
            catch (Exception)
            {

                throw;
            }
        }

        private void AddHashTagToDB(string iTweetID, string iHashTag)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            parameters.Add(String.Format("@{0}", TweetsHashtagsTableColumns[0]), iTweetID);
            parameters.Add(String.Format("@{0}", TweetsHashtagsTableColumns[1]), iHashTag);

            try
            {
                string sqlCmd = string.Format("select TweetID from TweetsHashtags where TweetID='{0}' and Hashtag='{1}'", iTweetID, iHashTag);
                DataTable result = SQLServerCommon.SQLServerCommon.ExecuteQuery(sqlCmd, connString);
                if (result.Rows.Count == 0)
                {
                    SQLServerCommon.SQLServerCommon.Insert("TweetsHashtags", connString, TweetsHashtagsTableColumns, parameters);
                }
            }
            catch (Exception)
            {

                throw;
            }
        }

        private void CreateEmptyDB()
        {
            int i = 0;
            try
            {
                //Create DB
                
                if (!SQLServerCommon.SQLServerCommon.IsDatabaseExists(connStringInitial, dbName))//connStringInitial, dbName))
                {
                    Console.WriteLine(string.Format("Creating DB: {0}", dbName));
                    SQLServerCommon.SQLServerCommon.ExecuteNonQuery(sqlCommandCreateDB, connStringInitial);
                    Console.WriteLine(string.Format("Creating DB: {0} - Succeeded.", dbName));

                    foreach (string tableName in tableNames)
                    {
                        Console.WriteLine(string.Format("Creating Table: {0}", tableName));
                        Console.WriteLine(string.Format("With the following schema: {0}", tableSchemas[i]));
                        SQLServerCommon.SQLServerCommon.ExecuteNonQuery(tableSchemas[i], connString);
                        Console.WriteLine(string.Format("Creating Table: {0} - Succeeded.", tableName));

                        i++;
                    }
                        
                }
                else
                {
                    //Check if all tables exist, if not, create them
                    
                    foreach (string tableName in tableNames)
                    {
                        if (SQLServerCommon.SQLServerCommon.IsTableExists(connString, dbName, tableName) == false)
                        {
                            Console.WriteLine(string.Format("Creating Table: {0}", tableName));
                            Console.WriteLine(string.Format("With the following schema: {0}", tableSchemas[i]));
                            SQLServerCommon.SQLServerCommon.ExecuteNonQuery(tableSchemas[i], connString);
                            Console.WriteLine(string.Format("Creating Table: {0} - Succeeded.", tableName));
                        }
                        i++;
                    }
                }

            }
            catch (Exception)
            {
                throw;
            }
        }

        #region DataHandlers
        private void GetTweets2Handler(ITweet iTweet, int iNumTweets)
        {
            if (_IsStreamStopped == false)
            {
                if (_Tweets.Count < iNumTweets)
                {
                    if (iTweet.Hashtags.Count > 0 && iTweet.Text.StartsWith("RT") && iTweet.Retweeting != null)
                    {
                        if (iTweet.Retweeting.RetweetCount > 1)
                        {
                            _Tweets.Add(iTweet.Retweeting);
                        }
                    }

                    if (_Tweets.Count == iNumTweets)
                    {
                        _SimpleStream.StopStream();
                    }
                }
            } 
        }

        private void GetTweetsHandler(TweetinCore.Interfaces.ITweet iTweet, int iNumTweets)
        {
            if (_Tweets.Count < iNumTweets)
            {
                if (iTweet.Hashtags.Count > 0 && iTweet.Text.StartsWith("RT") && iTweet.Retweeting != null)
                {
                    if (iTweet.Retweeting.RetweetCount > 1)
                    {
                        _Tweets.Add(iTweet.Retweeting);
                    }
                }
            }
            else
            {
                _SimpleStream.StopStream();
            }
        }

        private void HandleTweet(TweetinCore.Interfaces.ITweet iTweet, string iFileName)
        {
            using (StreamWriter writer = new StreamWriter(iFileName, true))
            {

                writer.WriteLine(iTweet.Text);
            }
        } 
        #endregion

        #region EventsHandlers
        void TimerElapsed(object sender, ElapsedEventArgs e)
        {
            _SimpleStream.StopStream();
            //_FilteredStream.StopStream();
            _IsStreamStopped = true;
        } 
        #endregion
        
    }
}
