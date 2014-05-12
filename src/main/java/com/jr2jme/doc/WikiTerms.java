package com.jr2jme.doc;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.mongojack.ObjectId;

import java.util.List;

/**
 * Created by JR2JME on 2014/04/07.
 */
public class WikiTerms {
    String title;
    int version;
    List<String> text;
    String editor;
    public WikiTerms(){}
    public WikiTerms(String title,int version,List<String> text,String editor){
        this.text=text;
        this.title=title;
        this.version=version;
        this.editor=editor;
    }

    public int getVersion() {
        return version;
    }

    public List<String> getText() {
        return text;
    }

    public String getEditor() {
        return editor;
    }

    public String getTitle() {
        return title;
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
