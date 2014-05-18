package com.jr2jme.Rev;

/**
 * Created by JR2JME on 2014/03/30.
 */

import java.util.ArrayList;
import java.util.List;


/**
 * Created by Hirotaka on 2014/03/26.
 */
public class Levenshtein3<T> {
    private EditNode2[] fp=null;
    private int m;
    private int n;
    private int offset;
    private List<T> A;
    private List<T> B;


    public List<String> diff(List<T> a, List<T> b){
        int delta;
        int size;
        m = a.size();
        n = b.size();
        A=a;
        B=b;
        Boolean reverse=false;
        if(n>m){//入れ替え
            List<T> x;
            x=A;
            A=B;
            B=x;
            int i;
            i=m;
            m=n;
            n=i;
            reverse=true;
        }
        offset=n;
        delta=m-n;
        size=n+m+1;
        if(n==0){
            List<String> list=new ArrayList<String>(size);
            String type="-";
            if(reverse){
                type="+";
            }
            for(T hoge:A){
                //String[] array = {hoge,type};
                list.add(type);
            }
            return list;
        }
// v[k] は k で到達可能な x の位置
        fp = new EditNode2[size];
        for(int i=0;i<size;i++){
            fp[i]=new EditNode2(null);
        }


        int p=-1;
        do {
            p = p + 1;
            for (int k = -p; k < delta; k++) {
                snake(k);
            }
            for (int k = delta + p; k > delta; k--) {
                snake(k);
            }
            snake(delta);
        } while(fp[delta+offset].getY()!=null&&fp[delta+offset].getY()<n);

        List<String> list=new ArrayList<String>();
        int a_index=m-1;
        int b_index=n-1;
        EditNode2 current = fp[delta+offset];
        for(EditTree i=current.getTree();i!=null;i=i.getPrevnode()){
            String type=i.getType();
            if(reverse){
                if(type.equals("+")){
                    //String[] str={B.get(b_index),"d"};
                    list.add("-");
                    //currenttype=0;
                    b_index--;
                }
                else if(type.equals("-")){
                    //String[] str={A.get(a_index),"i"};
                    list.add("+");

                    //currenttype=1;
                    a_index--;
                }
                else if(type.equals("|")){
                    //String[] str={A.get(a_index),"r"};
                    list.add("|");

                    //currenttype=2;
                    a_index--;
                    b_index--;
                }
            }
            else{
                list.add(type);
            }
        }
        List<String> list2=new ArrayList<String>();
        for(int i=list.size()-1;i>=0;i--){
            list2.add(list.get(i));
        }
        return list2;

    }

   private void snake(int k){
        if(k<-n||m<k){

        }
        else{
            EditNode2 current = fp[k+offset];

            if(k==-n){
                EditNode2 down=fp[k+1+offset];
                if(down.getY()!=null) {
                    current.setY(down.getY() + 1);
                }else{
                    current.setY(1);
                }
                current.addnode(down.getTree(), "+");
            }
            else if(k==m){
                EditNode2 slide = fp[k-1+offset];
                if(slide.getY()!=null) {
                    current.setY(slide.getY());
                }else{
                    current.setY(0);
                }
                current.addnode(slide.getTree(), "-");
            }
            else{
                EditNode2 slide = fp[k-1+offset];
                EditNode2 down  = fp[k+1+offset];
                if(slide.getY() ==null && down.getY()==null){
                    // どちらも未定義 => (0, 0)について
                    current.setY(0);
                } else if(down.getY() ==null || slide.getY() ==null){
                    // どちらかが未定義状態
                    if(down.getY() ==null){
                        current.setY(slide.getY());
                        current.addnode(slide.getTree(),"-");
                        //System.out.println("a");
                    } else {
                    current.setY(down.getY()+1);
                    current.addnode(down.getTree(),"+");
                        //System.out.println("z");

                    }
                } else {
                    // どちらも定義済み
                    if(slide.getY() > (down.getY()+1)){
                        current.setY(slide.getY());
                        current.addnode(slide.getTree(),"-");
                    } else {
                        current.setY(down.getY()+1);
                        current.addnode(down.getTree(),"+");

                    }
                }
            }
            int y=current.getY();
            int x=y+k;
            while(x < m && y < n && A.get(x).equals(B.get(y))){
                current.addnode(current.getTree(),"|");
                x++;
                y++;
            }
            current.setY(y);
        }

    }
}

class EditNode2{

    Integer y=null;
    EditTree tree= null;
    public EditNode2(Integer yx) {
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
    public Integer getY(){
        return y;
    }


}