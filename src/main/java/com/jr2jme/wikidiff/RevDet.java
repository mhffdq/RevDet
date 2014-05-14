package com.jr2jme.wikidiff;

import com.jr2jme.doc.WhoWrite;
import com.mongodb.*;
import net.java.sen.SenFactory;
import net.java.sen.StringTagger;
import net.java.sen.dictionary.Token;
import net.java.sen.filter.stream.CompositeTokenFilter;

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import java.util.concurrent.*;

//import org.atilika.kuromoji.Token;

//import net.java.sen.dictionary.Token;

//import org.atilika.kuromoji.Token;


public class RevDet {//Wikipediaのログから差分をとって誰がどこを書いたかを保存するもの リバート対応
    private static DBCollection coll;
    //private static JacksonDBCollection<WhoWrite,String> coll2;
    //private static JacksonDBCollection<InsertedTerms,String> coll3;//insert
    //private static JacksonDBCollection<DeletedTerms,String> coll4;//del&
    //private String wikititle = null;//タイトル
    static DB db=null;
    public static void main(String[] arg){
       // Set<String> aiming=fileRead("input.txt");
        MongoClient mongo=null;
        try {
            mongo = new MongoClient("dragons.db.ss.is.nagoya-u.ac.jp",27017);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        assert mongo != null;
        DB db=mongo.getDB("wikipediaDB_kondou");
        DBCollection dbCollection=db.getCollection("wikitext_Islam");
        coll=db.getCollection("wikitext_Islam");
        //coll = JacksonDBCollection.wrap(dbCollection, Wikitext.class, String.class);
        DBCollection dbCollection2=db.getCollection("editor_term_Islam");
        DBCollection dbCollection3=db.getCollection("Insertedterms_Islam");
        DBCollection dbCollection4=db.getCollection("DeletedTerms_Islam");

        //coll2 = JacksonDBCollection.wrap(dbCollection2, WhoWrite.class,String.class);
        //coll3 = JacksonDBCollection.wrap(dbCollection3, InsertedTerms.class,String.class);
        //coll4 = JacksonDBCollection.wrap(dbCollection4, DeletedTerms.class,String.class);


        RevDet wikidiff=new RevDet();
        //wikititle= title;//タイトル取得
        //Pattern pattern = Pattern.compile(title+"/log.+|"+title+"/history.+");
        Cursor cur=null;
        cur=wikidiff.wikidiff("アクバル");
        cur.close();
        mongo.close();
        System.out.println("終了:"+arg[0]);

    }

    public static Set fileRead(String filePath) {

        FileReader fr = null;
        BufferedReader br = null;
        Set<String> aiming= new HashSet<String>(350);
        try {
            fr = new FileReader(filePath);
            br = new BufferedReader(fr);

            String line;
            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                aiming.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return aiming;
    }

    public Cursor wikidiff(String title){
        //mongo準備
        final int NUMBER=50;
        //DBCollection dbCollection5=db.getCollection("Revert");
        ExecutorService exec = Executors.newFixedThreadPool(20);//マルチすれっど準備 20並列
        int offset=0;
        BasicDBObject findQuery = new BasicDBObject();//
        findQuery.put("title", title);
        findQuery.put("version",new BasicDBObject("$gt",offset));
        BasicDBObject sortQuery = new BasicDBObject();
        sortQuery.put("version", 1);
        DBCursor cursor = coll.find(findQuery).sort(sortQuery).limit(NUMBER);
        int version=1;
        List<WhoWrite> prevdata = null;
        long start=System.currentTimeMillis();
        List<String> prev_text=new ArrayList<String>();
        List<String> prevtext = new ArrayList<String>();
        WhoWriteResult[] resultsarray= new WhoWriteResult[20];//キューっぽいもの
        List<Integer[]>[] samearray=new List[20];
        int tail=0;
        int head;
        while(cursor.hasNext()) {//回す
            List<String> namelist=new ArrayList<String>(NUMBER+1);
            List<String> wikitext=new ArrayList<String>(NUMBER+1);
            //List<Future<List<String>>> futurelist = new ArrayList<Future<List<String>>>(NUMBER+1);
            for (DBObject dbObject:cursor) {//まず100件ずつテキストを(並列で)形態素解析
                wikitext.add((String)dbObject.get("text"));
                namelist.add((String)dbObject.get("name"));
            }
            cursor.close();
            List<List<String>> tasks2 = new ArrayList<List<String>>(wikitext.size()+1);
            for(String future:wikitext){//差分をとる
                List<String> parastr= Arrays.asList(future.split("\n\n|。"));
                List<String> prechange=new ArrayList<String>();
                List<String> nowchange=new ArrayList<String>();
                Levenshtein3 d = new Levenshtein3();
                List<String> diff = d.diff(prev_text, parastr);
                int a=0;
                int b=0;
                StringTagger tagger = SenFactory.getStringTagger(null);
                CompositeTokenFilter ctFilter = new CompositeTokenFilter();

                try {
                    ctFilter.readRules(new BufferedReader(new StringReader("名詞-数")));
                    tagger.addFilter(ctFilter);

                    ctFilter.readRules(new BufferedReader(new StringReader("記号-アルファベット")));
                    tagger.addFilter(ctFilter);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                List<String> curr_text = new ArrayList<String>();
                List<String> pret_text = new ArrayList<String>();
                List<Integer> addrow=new ArrayList<Integer>();
                List<Integer> delrow=new ArrayList<Integer>();
                List<Integer[]> samepare = new ArrayList<Integer[]>();
                for (String aDelta : diff) {//順番に見て，単語が残ったか追加されたかから，誰がどこ書いたか
                    //System.out.println(delta.get(x));
                    if (aDelta.equals("+")) {
                        List<Token> tokens = new ArrayList<Token>();
                        try {
                            tokens=tagger.analyze(parastr.get(a), tokens);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }


                        for(Token token:tokens){
                            curr_text.add(token.getSurface());
                        }
                        addrow.add(a);
                        a++;
                    } else if (aDelta.equals("-")) {
                        List<Token> tokens = new ArrayList<Token>();
                        try {
                            tokens=tagger.analyze(prev_text.get(a), tokens);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }


                        for(Token token:tokens){
                            pret_text.add(token.getSurface());
                        }
                        b++;
                    } else if (aDelta.equals("|")) {
                        Integer[] tmp={a,b};
                        samepare.add(tmp);
                        a++;
                        b++;
                    }
                }
                samearray[tail]=samepare;
                Integer[] pretmp={0,0};
                List<String> insterms= new ArrayList<String>();
                List<String> delterms=new ArrayList<String>();
                for(Integer[] tmp:samepare){
                    for(int i = 1;i<tmp[0]-pretmp[0];i++){
                        insterms.add(parastr.get(pretmp[0]+i));
                    }
                    for(int i = 1;i<tmp[1]-pretmp[1];i++){
                        delterms.add(parastr.get(pretmp[1]+i));
                    }

                    pretmp=tmp;
                }
                new DiffPos()
                diff=d.diff(pret_text,curr_text);

                version++;
                prev_text=parastr;
            }

            offset+=NUMBER;
            //System.out.println(offset);
            findQuery = new BasicDBObject();//
            findQuery.put("title", title);
            findQuery.put("version", new BasicDBObject("$gt", offset).append("$lte", offset + NUMBER));
            sortQuery = new BasicDBObject();
            sortQuery.put("version", 1);
            cursor = coll.find(findQuery).sort(sortQuery).limit(NUMBER);
        }
        exec.shutdown();
        try {
            exec.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(System.currentTimeMillis() - start);

        return cursor;

    }

    private WhoWriteResult whowrite(String title,String currenteditor,List<WhoWrite> prevdata,List<String> text,List<String> prevtext,List<String> delta,int ver) {//誰がどこを書いたか
        int a = 0;//この関数が一番重要
        int b = 0;
        WhoWriteResult whowrite = new WhoWriteResult(title, text, currenteditor, ver);
        for (String aDelta : delta) {//順番に見て，単語が残ったか追加されたかから，誰がどこ書いたか
            //System.out.println(delta.get(x));
            if (aDelta.equals("+")) {
                //System.out.println(text.get(a));
                whowrite.addaddterm(text.get(a));
                a++;
            } else if (aDelta.equals("-")) {
                whowrite.adddelterm(prevdata.get(b).getEditor(), prevtext.get(b));
                b++;
            } else if (aDelta.equals("|")) {
                //System.out.println(prevdata.getText_editor().get(b).getTerm());
                whowrite.remain(prevdata.get(b).getEditor(), text.get(a));
                a++;
                b++;
            }
        }
        whowrite.complete(prevdata);
        /*coll3.insert(whowrite.getInsertedTerms());
        for (DeletedTerms de : whowrite.getDeletedTerms().values()){
            coll4.insert(de);
        }*/
        return whowrite;


    }

}

class DiffPos {
    List<String> del;
    List<String> insert;
    int preue;
    int preshita;
    int nowue;
    int nowshita;
    public DiffPos(List<String> del, List<String> insert, int preue, int preshita, int nowue, int nowshita){
        this.del=del;
        this.insert=insert;
        this.preue=preue;
        this.preshita=preshita;
        this.nowue=nowue;
        this.nowshita=nowshita;
    }
}

