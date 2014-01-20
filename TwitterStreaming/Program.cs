using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;



namespace TwitterStreaming
{
    class Program
    {
        static void Main(string[] args)
        {
            TwitterStreamer twitterStreamer = new TwitterStreamer("https://stream.twitter.com/1.1/statuses/sample.json", 5000);
            Stopwatch timer = new Stopwatch();

            timer.Start();
            Console.WriteLine(string.Format("Started: {0}", DateTime.Now));
            twitterStreamer.GetTweets();
            Console.WriteLine(string.Format("Ended: {0}", DateTime.Now));
            timer.Stop();

            Console.WriteLine(string.Format("Total time collecting Tweets: {0}", timer.Elapsed));


            //twitterStreamer.PrintTweets();
        }
    }
}
