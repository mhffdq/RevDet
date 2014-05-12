package com.jr2jme.doc;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.mongojack.ObjectId;

/**
 * Created by Hirotaka on 2014/04/08.
 */
public class WhoWrite {
    String editor;
    String title;
    int version;
    String term;
    int order;
    public WhoWrite(){
    }
    public WhoWrite(String editors,String title,int id,String term,int order){
        this.editor=editors;
        this.title=title;
        version=id;
        this.term=term;
        this.order=order;
    }

    public String getTerm() {
        return term;
    }

    public int getOrder() {
        return order;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public void setEditor(String text_editor) {
        this.editor = text_editor;
    }
    public String getEditor(){
        return editor;
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