using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Streaminvi;
using TwitterToken;
using TweetinCore;
using TweetinCore.Interfaces.TwitterToken;
using System.Timers;
using System.IO;

namespace TwitterStreaming
{
    class TwitterStreamer
    {
        string ConsumerKey1 = System.Configuration.ConfigurationManager.AppSettings["ConsumerKey1"];
        string ConsumerSecret1 = System.Configuration.ConfigurationManager.AppSettings["ConsumerSecret1"];
        string AccessToken1 = System.Configuration.ConfigurationManager.AppSettings["AccessToken1"];
        string AccessTokenSecret1 = System.Configuration.ConfigurationManager.AppSettings["AccessTokenSecret1"];

        #region Members
        SimpleStream _SimpleStream;
        FilteredStream _FilteredStream;
        //UserStream _UserStream;
        IToken _Token;
        Timer _Timer = new Timer();
        bool _IsStreamStopped = false;
        Action<TweetinCore.Interfaces.ITweet, string> _DataHandlerMethod; 
        #endregion

        /// <summary>
        /// C'tor
        /// </summary>
        /// <param name="iUrl">The URL to stream from</param>
        /// <param name="iPeriod">The period of time in miliseconds in which streaming will occur</param>
        public TwitterStreamer(string iUrl, double iPeriod )
        {
            _Token = new Token(AccessToken1, AccessTokenSecret1, ConsumerKey1, ConsumerSecret1);
            //_SimpleStream = new SimpleStream(iUrl);
            _FilteredStream = new FilteredStream();

            _FilteredStream.AddTrack("#android");
            //_UserStream = new UserStream();
            _Timer.Interval = iPeriod;
            _Timer.AutoReset = true; //Stops it from repeating
            _Timer.Start();
            _Timer.Elapsed += new ElapsedEventHandler(TimerElapsed);
        }

        public void GetStreamIntoFile()
        {
            string filename = "Stream_" + DateTime.Now.ToString("dd.MM.yyyy.HH.mm.ss.ffff");
            _DataHandlerMethod = HandleTweet;

            //_SimpleStream.StartStream(_Token, x => _DataHandlerMethod(x, filename));
            _FilteredStream.StartStream(_Token, x => _DataHandlerMethod(x, filename));
        }

        private void HandleTweet(TweetinCore.Interfaces.ITweet iTweet, string iFileName)
        {
            using (StreamWriter writer = new StreamWriter(iFileName, true))
            {
                writer.WriteLine(iTweet.Text);
            }
        }

        void TimerElapsed(object sender, ElapsedEventArgs e)
        {
            //_SimpleStream.StopStream();
            _FilteredStream.StopStream();
            _IsStreamStopped = true;
        }
        
    }
}
