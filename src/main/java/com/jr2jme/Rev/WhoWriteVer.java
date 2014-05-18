package com.jr2jme.Rev;

import com.jr2jme.doc.WhoWrite;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Hirotaka on 2014/04/25.
 */
public class WhoWriteVer {
    List<WhoWrite> whowritelist=new ArrayList<WhoWrite>();
    int version;
    public WhoWriteVer(int ver){
        version=ver;
    }
    public void addwhowrite(WhoWrite who){
        this.whowritelist.add(who);
    }

    public int getVersion() {
        return version;
    }

    public List<WhoWrite> getWhowritelist() {
        return whowritelist;
    }
}
