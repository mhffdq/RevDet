package com.jr2jme.Rev;

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
        List<DelTerm> dellist = new ArrayList<DeleteTerm>();
        List<String>[] difflist = new List[20];
        int tail=0;
        int head;
        while(cursor.hasNext()) {//回す
            List<String> namelist=new ArrayList<String>(NUMBER+1);
            List<Future<List<String>>> futurelist = new ArrayList<Future<List<String>>>(NUMBER+1);
            //List<Future<List<String>>> futurelist = new ArrayList<Future<List<String>>>(NUMBER+1);
            for (DBObject dbObject:cursor) {//まず100件ずつテキストを(並列で)形態素解析
                namelist.add((String)dbObject.get("name"));
                futurelist.add(exec.submit(new Kaiseki((String)dbObject.get("text"))));
            }
            cursor.close();
            List<Future<List<String>>> tasks = new ArrayList<Future<List<String>>>(futurelist.size()+1);
            int i=0;
            for(Future<List<String>> future:futurelist) {//差分をとる
                try {
                    List<String> text = future.get();
                    tasks.add(exec.submit(new CalDiff(text, prev_text, title, version, namelist.get(i))));
                    i++;
                    version++;
                    prev_text = text;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            for (Future<List<String>> aDelta : tasks) {//順番に見て，単語が残ったか追加されたかから，誰がどこ書いたか
                try {
                    List<Integer> instermpos = new ArrayList<Integer>();
                    List<Insterm> insterm = new ArrayList<String>();
                    List<Integer> deltermpos = new ArrayList<Integer>();
                    List<String> delterm = new ArrayList<String>();
                    List<Integer[]> samepare = new ArrayList<Integer[]>();
                    int a = 0;
                    int b = 0;
                    int c = 0;
                    for (String type : aDelta.get()) {
                        if (type.equals("+")) {
                            instermpos.add(a);
                            insterm.add(new Instrerm(futurelist.get(c).get().get(a),a));
                            a++;
                        } else if (type.equals("-")) {
                            deltermpos.add(b);
                            delterm.add(futurelist.get(c).get().get(a));
                            whowrite.delete(b,editor,version);//追加した単語には位置とかいろいろ情報合って分かるので適当にやる
                            b++;
                        } else if (type.equals("|")) {
                            Integer[] tmp = {a, b};
                            samepare.add(tmp);
                            a++;
                            b++;
                        }
                    }
                    for(Insterm term:insterm) {//今消された単語と同じ単語があるかどうか
                        for(DeleteTerm del:dellist){//全消された単語の中から
                            if (del.getterm().eqals(term.getterm())) {//確かめて
                                int ue = del.getue();//文章の上と
                                int shita = del.getshita();//下で
                                for (int x = nowversion - delterm.getversion(); x < nowversion; x++) {//矛盾が出ないか確かめる
                                    int aa = 0;
                                    int bb = 0;
                                    for (int y = 0; y < difflist[index + x].size(); y++) {
                                        String type = difflist[index + x].get(y);
                                        if (type.equals("+")) {
                                            ue++;
                                            shita++;
                                            aa++;
                                        } else if (type.equals("-")) {
                                            ue--;
                                            shita--;

                                            bb++;
                                        } else if (type.equals("|")) {
                                            Integer[] tmp = {a, b};
                                            samepare.add(tmp);
                                            if (aa > ue) {
                                                break;//?
                                            }
                                            if (aa > shita) {
                                                break;//?
                                            }
                                            aa++;
                                            bb++;
                                        }
                                    }
                                }
                                if (term.pos > ue && term.pos < shita) {
                                    revertterm;
                                }
                            }
                        }
                    }
                    difflist[tail]=aDelta.get();
                    samearray[tail] = samepare;

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                offset += NUMBER;
                //System.out.println(offset);
                findQuery = new BasicDBObject();//
                findQuery.put("title", title);
                findQuery.put("version", new BasicDBObject("$gt", offset).append("$lte", offset + NUMBER));
                sortQuery = new BasicDBObject();
                sortQuery.put("version", 1);
                cursor = coll.find(findQuery).sort(sortQuery).limit(NUMBER);
            }
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
    public DiffPos(List<String> insert, List<String> del, int preue, int preshita, int nowue, int nowshita){
        this.del=del;
        this.insert=insert;
        this.preue=preue;
        this.preshita=preshita;
        this.nowue=nowue;
        this.nowshita=nowshita;
    }
}

class Kaiseki implements Callable<List<String>> {//形態素解析
    String wikitext;//gosenだとなんか駄目だった→kuromojimo別のでダメ
    public Kaiseki(String wikitext){
        this.wikitext=wikitext;
    }
    @Override
    public List<String> call() {

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

        List<Token> tokens = new ArrayList<Token>();
        try {
            tokens=tagger.analyze(wikitext, tokens);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<String> current_text = new ArrayList<String>(tokens.size());

        for(Token token:tokens){

            current_text.add(token.getSurface());
        }

        return current_text;
    }


}

class CalDiff implements Callable<List<String>> {//差分
    List<String> current_text;
    List<String> prev_text;
    public CalDiff(List<String> current_text,List<String> prev_text,String title,int version,String name){
        this.current_text=current_text;
        this.prev_text=prev_text;
    }
    @Override
    public List<String> call() {//並列で差分
        Levenshtein3 d = new Levenshtein3();
        List<String> diff = d.diff(prev_text, current_text);
        return diff;
    }
}

class InsTerm {
    String term;
    Integer pos;
    public InsTerm(String term,Integer pos){
        this.term=term;
        this.pos=pos;
    }
}
class DelTerm{
    String term;
    List<DelPos> posList=new ArrayList<DelPos>();
    public DelTerm(String term,int ue,int shita){
        this.term=term;
        posList.add(new DelPos(ue,shita));

    }
}
class DelPos{
    int ue;
    int shita;
    public DelPos(int ue,int shita){
        this.ue=ue;
        this.shita=shita;
    }
    public int getUe() {
        return ue;
    }

    public int getShita() {
        return shita;
    }
}