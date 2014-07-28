package TF_DF.n;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws IOException {

        Map<String, HashMap<String, Integer>> normal = ReadFiles.NormalTFOfAll("/home/duanfa/workspace/demon/src/TF_DF/file");
        for (String filename : normal.keySet()) {
            System.out.println("fileName " + filename);
            System.out.println("TF " + normal.get(filename).toString());
        }

        System.out.println("-----------------------------------------");

        Map<String, HashMap<String, Float>> notNarmal = ReadFiles.tfOfAll("/home/duanfa/workspace/demon/src/TF_DF/file");
        for (String filename : notNarmal.keySet()) {
            System.out.println("fileName " + filename);
            System.out.println("TF " + notNarmal.get(filename).toString());
        }

        System.out.println("-----------------------------------------");

        Map<String, Float> idf = ReadFiles.idf("/home/duanfa/workspace/demon/src/TF_DF/file");
        for (String word : idf.keySet()) {
            System.out.println("keyword :" + word + " idf: " + idf.get(word));
        }

        System.out.println("-----------------------------------------");

        Map<String, HashMap<String, Float>> tfidf = ReadFiles.tfidf("/home/duanfa/workspace/demon/src/TF_DF/file");
        for (String filename : tfidf.keySet()) {
            System.out.println("fileName " + filename);
            System.out.println(tfidf.get(filename));
        }
    }
}