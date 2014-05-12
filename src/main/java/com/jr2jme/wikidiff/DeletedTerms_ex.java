package com.jr2jme.wikidiff;

/**
 * Created by Hirotaka on 2014/04/09.
 */
/*public class DeletedTerms_ex extends com.jr2jme.doc.DeletedTerms {

    Map<String,Integer> wordcount=new HashMap<String, Integer>();
    Map<String,Map<String,Integer>> delterms=new HashMap<String, Map<String, Integer>>();

    public DeletedTerms_ex(String title,String editor,int version){
        super.title=title;
        super.editor=editor;
        super.version=version;
    }

    public DeletedTerms_ex(DeletedTerms superdel){
        super.title = superdel.getTitle();
        super.editor = superdel.getEditor();
        super.version = superdel.getVersion();
        wordcount=new HashMap<String, Integer>();
        delterms=new HashMap<String, Map<String, Integer>>(superdel.getTerms().size());
        for(Delete delete:superdel.getTerms()){
            Map<String,Integer> tempmap=new HashMap<String, Integer>();
            for(TermCount termcount:delete.getWords()){
                if(wordcount.containsKey(termcount.getTerm())) {
                    wordcount.put(termcount.getTerm(), wordcount.get(termcount.getTerm()) + termcount.getCount());
                }
                else{
                    wordcount.put(termcount.getTerm(),termcount.getCount());
                }
                tempmap.put(termcount.getTerm(),termcount.getCount());
            }
            delterms.put(delete.getDeletededitor(),tempmap);
        }

    }

    public DeletedTerms makeDeletedTerms(){
        List<Delete> dellist=new ArrayList<Delete>();
        for(Map.Entry<String,Map<String,Integer>> entry:delterms.entrySet()){
            List<TermCount> termlist = new ArrayList<TermCount>();
            for(Map.Entry<String,Integer> termc:entry.getValue().entrySet()){
                termlist.add(new TermCount(termc.getKey(),termc.getValue()));
            }
            dellist.add(new Delete(entry.getKey(),termlist));
        }
        return new DeletedTerms(this.title,this.editor,dellist,this.version);
    }


    public void add(String editor,String term){
        Map<String,Integer> temp=null;
        if(delterms.containsKey(editor)){
            temp=delterms.get(editor);
            if(temp.containsKey(term)){
                temp.put(term,temp.get(term)+1);

            }
            else{
                temp.put(term,1);
            }
        }else{
            temp=new HashMap<String,Integer>();
            temp.put(term,1);
            delterms.put(editor, temp);
        }
        if(wordcount.containsKey(term)){
            wordcount.put(term,wordcount.get(term)+1);
        }else{
            wordcount.put(term,1);
        }
    }




    public boolean equalswords(Map<String,Integer> a){
        return wordcount.equals(a);
    }
}*/

