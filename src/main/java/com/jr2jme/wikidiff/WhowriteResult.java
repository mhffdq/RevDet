package com.jr2jme.wikidiff;

import com.jr2jme.doc.DeletedTerms;
import com.jr2jme.doc.InsertedTerms;
import com.jr2jme.doc.WhoWrite;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Hirotaka on 2014/04/21.
 */
public class WhoWriteResult {
    WhoWriteVer whoWritever=null;
    private InsertedTerms insertedTerms=null;
    private Map<String,DeletedTerms> deletedTerms=null;
    private List<String> dellist=new ArrayList<String>();//消された編集者のリスト
    List<String> delwordcount = new ArrayList<String>();
    String title;
    String editor;
    int version;
    int order=0;
    //private String editor;
    List<String> text;//比較用ハッシュ
    public WhoWriteResult(String title,List<String> text,String editor,int ver){
        whoWritever=new WhoWriteVer(ver);
        insertedTerms = new InsertedTerms(title,editor,ver);
        deletedTerms=new HashMap<String, DeletedTerms>();
        this.editor=editor;
        this.title=title;
        this.version=ver;

        this.text=text;

    }


    public String getEditor(){
        return editor;
    }
    public List<String> getWordcount() {
        return delwordcount;
    }

    public void adddelterm(String preeditor,String term){

        dellist.add(preeditor);
        if(deletedTerms.containsKey(preeditor)) {
            deletedTerms.get(preeditor).addterm(term);
        }
        else{
            DeletedTerms deletedterms=new DeletedTerms(title,editor,preeditor,version);
            deletedterms.addterm(term);
            deletedTerms.put(preeditor,deletedterms);
        }
        delwordcount.add(term);
    }

    public void addaddterm(String term){
        whoWritever.addwhowrite(new WhoWrite(editor,title,version,term,order));
        insertedTerms.add(term);
        order++;
    }
    public void remain(String preeditor,String term){
        whoWritever.addwhowrite(new WhoWrite(preeditor, title, version, term, order));
        order++;
    }
    public WhoWriteVer getWhoWritever() {
        return whoWritever;
    }

    public List<String> getDellist() {
        return dellist;
    }

    public List<String> getText() {
        return text;
    }

    public InsertedTerms getInsertedTerms() {
        return insertedTerms;
    }

    public Map<String,DeletedTerms> getDeletedTerms() {
        return deletedTerms;
    }

    public void complete(List<WhoWrite> prevdata){
        Map<String,Integer> deletecount=new HashMap<String, Integer>();
        for(String deleditor:dellist){//消された編集者
            if(deletecount.containsKey(deleditor)){
                deletecount.put(deleditor,deletecount.get(deleditor)+1);
            }else{
                deletecount.put(deleditor,1);
            }
        }
        for(Map.Entry<String,DeletedTerms> deleditor:deletedTerms.entrySet()){//消された単語数登録
            deleditor.getValue().setDelnum(deletecount.get(deleditor.getKey()));
            int count=0;
            for(WhoWrite who:prevdata){
                if(deleditor.getKey().equals(who.getEditor())){
                    count++;
                }
            }
            deleditor.getValue().setTotal(count);

        }
    }

    public boolean compare(WhoWriteResult ddd){

        if((this.insertedTerms.getTerms().equals(ddd.getWordcount())&&this.getWordcount().equals(ddd.getInsertedTerms().getTerms()))) {//ある編集と逆の操作をしているか
            //取り消しだった場合
            /*for(Map.Entry<String,Integer> hoge:this.deletedTerms.wordcount.entrySet()){
                System.out.println(hoge.getKey());
            }*/
            return true;
        }else{
            return false;
        }
    }
    public boolean comparehash(List<String> hash){//同じか
        return text.equals(hash);
    }
    private String String2MD5(String key){
        byte[] hash = null;
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(key.getBytes());
            hash = md.digest();
        } catch (NoSuchAlgorithmException e) {
            //
        }
        return hashByte2MD5(hash);
    }

    private String hashByte2MD5(byte []hash) {
        StringBuilder hexString = new StringBuilder();
        for (byte aHash : hash) {
            if ((0xff & aHash) < 0x10) {
                hexString.append("0").append(Integer.toHexString((0xFF & aHash)));
            } else {
                hexString.append(Integer.toHexString(0xFF & aHash));
            }
        }

        return hexString.toString();
    }

}

