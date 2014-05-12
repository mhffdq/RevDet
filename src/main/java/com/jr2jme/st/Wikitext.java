package com.jr2jme.st;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.mongojack.ObjectId;

import java.util.Date;

/**
 * Created by Hirotaka on 2014/03/23.
 */
public class Wikitext {
    String title;
    Date date;
    String name;
    String text;
    String comment;
    int revid;
    int version;
    public Wikitext(){//forjackson dummy

    }
    public Wikitext(String titlea, Date datea, String namea, String texta, int id, String com,int ver){
        title=titlea;
        date=datea;
        name=namea;
        text=texta;
        revid=id;
        comment=com;
        version=ver;
    }
    public void setDate(Date date) {
        this.date = date;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setTitle(String title) {
        this.title = title;
    }
    public String getTitle(){
        return title;
    }

    public Date getDate() {
        return date;
    }

    public String getName() {
        return name;
    }

    public String getText() {
        return text;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setRevid(int revid) {
        this.revid = revid;
    }

    public int getRevid() {
        return revid;
    }

    public String getComment() {
        return comment;
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
