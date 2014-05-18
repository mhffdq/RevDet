package com.jr2jme.Rev;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Hirotaka on 2014/03/26.
 */
public class Levenshtein2 {
    static public List<String> diff(List<String> a,List<String> b){
        int m = a.size();
        int n = b.size();
        int max= n+m;
// v[k] は k で到達可能な x の位置
        EditNode[] edittree = new EditNode[max*2+1];
        for(int i=0;i<max+1;i++){
            edittree[i]=new EditNode(0);
        }
        EditNode current=null;
        EditNode add=null;
        EditNode del=null;
        Boolean escape=false;
        for(int d = 0;d<max+1;d++){
            for(int k = -d;k<=d;k+=2){
                //System.out.println("d="+d+"k="+k);
                if(k < -n||m < k){
                    //System.out.println("e");
                    continue;
                }
                current=edittree[k+n];
                if(d!=0){
                    if(k==-d||k==-n){
                        add=edittree[k+1+n];
                        current.setY(add.getY()+1);
                        current.addnode(add.getTree(),"+");
                        //System.out.println("a");
                    }
                    else if(k==d||k==m){
                        del=edittree[k-1+n];
                        current.setY(del.getY());
                        current.addnode(del.getTree(),"-");
                        //System.out.println("b");
                    }
                    else {
                        if(edittree[k-1+n].getY()>edittree[k+1+n].getY()+1){
                            del=edittree[k-1+n];
                            current.setY(del.getY());
                            current.addnode(del.getTree(),"-");
                            //System.out.println("c");
                        }else{
                            add=edittree[k+1+n];
                            current.setY(add.getY()+1);
                            current.addnode(add.getTree(),"+");
                            //System.out.println("d");
                        }
                    }
                }

                int x = current.getY()+k;
                int y = current.getY();
                //System.out.print(x);
                //System.out.print(" "+y+"\n");

                while(x < m&& y < n&& (a.get(x).equals(b.get(y)) )){
                    //System.out.println(a.get(x)+b.get(y));
                    current.addnode(current.getTree(),"|");
                    x++;
                    y++;
                    //System.out.println(a.get(x-1));
                }
                current.setY(y);
                if(x>=m&&y>=n){//完成
                    escape=true;
                    //System.out.println(d);
                    break;
                }
            }
            if(escape){//完成
                break;
            }
        }
        List<String> list=new ArrayList<String>();
        int a_index=m-1;
        int b_index=n-1;

        for(EditTree i=current.getTree();i!=null;i=i.getPrevnode()){
            //System.out.println(current.getPrevnode());
            list.add(i.getType());

        }
        List<String> list2=new ArrayList<String>();
        for(int i=list.size()-1;i>=0;i--){
            list2.add(list.get(i));
        }
        return list2;
    }
}

class EditNode{

    int y=0;
    EditTree tree= null;
    public EditNode(int yx) {
        y = yx;
    }
    public void addnode(EditTree t,String type){
        tree=new EditTree(t,type);
    }
    public EditTree getTree(){
        return tree;
    }
    public void setY(int y){
        this.y=y;
    }
    public int getY(){
        return y;
    }


}

class EditTree{
    EditTree prevtree=null;
    String type="";
    EditTree(EditTree t,String type){
        prevtree=t;
        this.type=type;
    }
    public EditTree getPrevnode(){
        return prevtree;
    }

    public void setType(String s){
        type = s;
    }
    public String getType(){
        return type;
    }
}
