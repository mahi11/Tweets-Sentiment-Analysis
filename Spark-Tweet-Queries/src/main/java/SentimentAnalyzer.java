
import com.mongodb.BasicDBObject;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONObject;


public class SentimentAnalyzer {

    static MongoInsert mongoInsert = new MongoInsert();
    static StanfordCoreNLP pipeline;
    static Properties props;


    public SentimentAnalyzer()
    {
        mongoInsert.createDBCollection();
        createPipeLine();
    }

    public static void createPipeLine()
    {
        if(props==null){
            props = new Properties();
            props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
            pipeline = new StanfordCoreNLP(props);
        }

    }

    public TweetWithSentiment findSentiment(String line, Date date,String[] hashtag,String source,String sname,String uname,String lang,String tzone,Long rtweetcount,int focount,int frcount,String teamtag){

        int mainSentiment = 0;
        if (line != null && line.length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(line);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }

        TweetWithSentiment tweetWithSentiment = new TweetWithSentiment(line, toCss(mainSentiment));

        BasicDBObject tweetObject= new BasicDBObject();
        tweetObject.put("tweet",line);
        tweetObject.put("date",date);
        tweetObject.put("sentiment",mainSentiment);
        tweetObject.put("hashtag",hashtag);
        tweetObject.put("teamtag",teamtag);
        tweetObject.put("source",source);
        tweetObject.put("username",uname);
        tweetObject.put("screenname",sname);
        tweetObject.put("timezone",tzone);
        tweetObject.put("lang",lang);
        tweetObject.put("rcount",rtweetcount);
        tweetObject.put("focount",focount);
        tweetObject.put("frcount",frcount);
        mongoInsert.insertTweet(tweetObject);


        return tweetWithSentiment;

    }
    public String[] dateandtime(Date createstatus){


        String[] result= new String[8];

        SimpleDateFormat hDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0 z");
        SimpleDateFormat year = new SimpleDateFormat("yyyy");
        SimpleDateFormat month = new SimpleDateFormat("MM");
        SimpleDateFormat day = new SimpleDateFormat("dd");
        SimpleDateFormat hour = new SimpleDateFormat("HH");
        SimpleDateFormat min = new SimpleDateFormat("mm");
        SimpleDateFormat sec = new SimpleDateFormat("ss");
        SimpleDateFormat timezone = new SimpleDateFormat("z");

        String date1 = hDateFormat.format(createstatus);
        String year1 = year.format(createstatus);
        String month1 = month.format(createstatus);
        String day1 = day.format(createstatus);
        String timezone1 = timezone.format(createstatus);
        String hour1 = hour.format(createstatus);
        String min1 = min.format(createstatus);
        String sec1 = sec.format(createstatus);

        result[0]=sec1;
        result[1]=min1;
        result[2]=hour1;
        result[3]=day1;
        result[4]=month1;
        result[5]=year1;
        result[6]=timezone1;
        result[7]=date1;
        return result;
    }

    public String sourceextractor(String sourcetext){

        String[] x = sourcetext.split(">");

//        System.out.println("\n");
//        System.out.println(x[1]);
        String[] y = x[1].split("<");
//        System.out.println(y[0]);
        String sourceresult;
        if(y[0].contains(" ")){
            String[] z = y[0].split(" ");
            sourceresult = z[2];
//            System.out.println(z[2]);
        }
        else{
            sourceresult= y[0];
//            System.out.println(y[0]);
        }


        return sourceresult;
    }
       private String toCss(int sentiment) {
        switch (sentiment) {
            case 0:
                return "sentiment : very negative";
            case 1:
                return "sentiment : negative";
            case 2:
                return "sentiment : neutral";
            case 3:
                return "sentiment : positive";
            case 4:
                return "sentiment : very positive";
            default:
                return "";
        }
    }

    public static void main(String[] args) {

    }
}
