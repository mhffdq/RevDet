package com.jr2jme.doc;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.mongojack.ObjectId;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Hirotaka on 2014/04/09.
 */
public class DeletedTerms {
    protected String title;
    protected String editorFrom;
    String editorTo;
    List<String> delterms=new ArrayList<String>();
    protected int version;
    int total=0;
    int delnum=0;

    //Map<String,Integer> wordcount;

    public DeletedTerms(String title, String editorFrom, String editorTo,int version) {
        this.title = title;
        this.editorFrom = editorFrom;
        this.version = version;
        this.editorTo=editorTo;
    }

    public void addterm(String term){
        delterms.add(term);
    }
    public String getTitle() {
        return title;
    }

    public String getEditorFrom() {
        return editorFrom;
    }

    public String getEditorTo(){
        return editorTo;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public void setDelnum(int delnum) {
        this.delnum = delnum;
    }

    public List<String> getTerms() {
        return delterms;
    }

    public int getVersion() {
        return version;
    }

    public int getDelnum() {
        return delnum;
    }

    public int getTotal() {
        return total;
    }

    private String id;

    @ObjectId
    @JsonProperty("_id")
    public String getId() {
        return id;
    }

    @ObjectId
    @JsonProperty("_id")
    public void setId(String id) {
        this.id = id;
    }



}



