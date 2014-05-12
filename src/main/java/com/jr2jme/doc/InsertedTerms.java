package com.jr2jme.doc;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.mongojack.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Hirotaka on 2014/04/09.
 */
public class InsertedTerms {
    String title;
    String editor;
    List<String> terms=new ArrayList<String>();
    int version;
    public InsertedTerms(String title,String editor,int version){
        this.title=title;
        this.editor=editor;
        this.version=version;
    }
    public void add(String str){
        terms.add(str);
    }

    public String getTitle() {
        return title;
    }

    public String getEditor() {
        return editor;
    }

    public List<String> getTerms() {
        return terms;
    }

    public int getVersion() {
        return version;
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

    public boolean equalswords(Map<String,Integer> a){
        return this.getTerms().equals(a);
    }
}


