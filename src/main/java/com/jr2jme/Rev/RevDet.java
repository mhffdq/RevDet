package com.jr2jme.Rev;

import com.mongodb.*;
import net.java.sen.SenFactory;
import net.java.sen.StringTagger;
import net.java.sen.dictionary.Token;
import net.java.sen.filter.stream.CompositeTokenFilter;

import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
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
        int nowversion=1;
        List<WhoWrite> prevdata = null;
        long start=System.currentTimeMillis();
        List<String> prev_text=new ArrayList<String>();
        List<String> prevtext = new ArrayList<String>();
        Map<String,List<DelPos>> delmap = new HashMap<String, List<DelPos>>();
        List<List<String>> difflist = new ArrayList<List<String>>();
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
            i=0;
            int c = 0;
            WhoWrite prevwrite=null;
            for (Future<List<String>> aDelta : tasks) {//順番に見て，単語が残ったか追加されたかから，誰がどこ書いたか
                try {
                    List<InsTerm> insterm = new ArrayList<InsTerm>();
                    int tmp=0;
                    WhoWrite whowrite=new WhoWrite();
                    int a = 0;
                    int b = 0;
                    List<String> yoyaku=new ArrayList<String>();
                    List<String> yoyakued=new ArrayList<String>();
                    List<Integer> yoyakuver=new ArrayList<Integer>();
                    List<String> edlist = new ArrayList<String>();
                    for (String type : aDelta.get()) {
                        if (type.equals("+")) {
                            edlist.add(namelist.get(i));
                            insterm.add(new InsTerm(futurelist.get(c).get().get(a),a,namelist.get(i)));
                            a++;
                        } else if (type.equals("-")) {
                            yoyakued.add(prevwrite.getEditorList().get(b));
                            yoyakuver.add(prevwrite.getVerlist().get(b));
                            yoyaku.add(prevtext.get(b));//リバートされるかもしれないリストに突っ込む準備
                            //delterm.add(futurelist.get(c).get().get(a));
                            //whowrite.delete(b,namelist.get(i),version);//追加した単語には位置とかいろいろ情報あって分かるので適当にやる
                            b++;
                        } else if (type.equals("|")) {
                            for(int p=0;p<yoyaku.size();p++){
                                if(delmap.containsKey(yoyaku.get(p))){
                                    List<DelPos> list = delmap.get(yoyaku.get(p));
                                    DelPos pos=new DelPos(nowversion,tmp,b,namelist.get(i),yoyakuver.get(p),yoyakued.get(p));
                                    list.add(pos);
                                }
                                else{
                                    List<DelPos> list =new ArrayList<DelPos>();
                                    DelPos pos=new DelPos(nowversion,tmp,b,namelist.get(i),yoyakuver.get(p),yoyakued.get(p));
                                    list.add(pos);
                                    delmap.put(yoyaku.get(p),list);
                                }
                            }
                            tmp=b;
                            a++;
                            b++;
                        }

                    }
                    prevwrite=whowrite;
                    for(InsTerm term:insterm) {//今消された単語と同じ単語があるかどうか
                        for(Map.Entry<String,List<DelPos>> del:delmap.entrySet()){//全消された単語の中から
                            if (del.getKey().equals(term.getTerm())) {//確かめて
                                for(DelPos delpos:del.getValue()) {
                                    int ue = delpos.getue();//文章の上と
                                    int shita = delpos.getshita();//下で
                                    int delcount=0;
                                    for (int x = nowversion - delpos.getVersion(); x < nowversion; x++) {//矛盾が出ないか確かめる
                                        int aa = 0;
                                        for (int y = 0; y < difflist.get(x).size(); y++) {
                                            String type = difflist.get(x).get(y);
                                            if (type.equals("+")) {
                                                ue++;
                                                shita++;
                                                aa++;
                                            } else if (type.equals("-")) {
                                                ue--;
                                                shita--;
                                                delcount++;
                                            } else if (type.equals("|")) {
                                                if (aa > ue) {
                                                    break;//?
                                                }
                                                if (aa > shita) {
                                                    break;//?
                                                }
                                                aa++;
                                            }
                                        }
                                    }
                                    if (term.pos > ue && term.pos < shita) {
                                        term.revertterm(delpos);
                                        del.getValue().remove(delpos);
                                    }
                                }
                            }
                        }
                    }
                    difflist.add(aDelta.get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                offset += NUMBER;
                nowversion++;
                i++;
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
    String editor;
    Integer pos;
    Boolean isRevert=false;
    Integer revedver=null;
    String reveded;
    public InsTerm(String term,Integer pos,String editor){
        this.term=term;
        this.pos=pos;
    }
    public void revertterm(DelPos delpos){
        isRevert=true;
        revedver=delpos.getVersion();
        reveded=delpos.getEditor();
    }

    public String getTerm() {
        return term;
    }
}

class Delterm{
    String term;
    String editor;
    int revedver=0;
    String reveded;
}

class DelPos{
    int ue;
    int shita;
    int version;
    String editor;
    int deledver;
    String delededitor;
    public DelPos(int version,int ue,int shita,String editor,int deledver,String delededitor){
        this.ue=ue;
        this.shita=shita;
        this.version=version;
        this.delededitor=delededitor;
        this.editor=editor;
        this.deledver=deledver;
    }
    public int getue() {
        return ue;
    }

    public int getshita() {
        return shita;
    }

    public int getVersion() {
        return version;
    }

    public String getEditor() {
        return editor;
    }
}

class WhoWrite{
    List<String> wikitext=new ArrayList<String>();
    List<String> editorList=new ArrayList<String>();
    List<Integer> verlist= new ArrayList<Integer>();
    /*public void delete(int pos,String editor,int version){
        wikitext.remove(pos);
        editorList.remove(pos);
        verlist.remove(pos);
    }*/
    public void add(String term,String editor,int ver){
        wikitext.add(term);
        editorList.add(editor);
        verlist.add(ver);
    }

    public List<Integer> getVerlist() {
        return verlist;
    }

    public List<String> getEditorList() {
        return editorList;
    }
}