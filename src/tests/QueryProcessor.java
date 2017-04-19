package tests;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import global.Descriptor;
import global.NID;
import global.PageId;
import queryPojo.*;
public class QueryProcessor {
    int pageno;
    int slotno;
    public List<EdgeQueryPojo> PathExpression2(String input)
    {
        String paths[]=new String[1000];
        List<EdgeQueryPojo> list=new LinkedList<EdgeQueryPojo>();
        paths=input.split("/");
        for(int i=0;i<paths.length;i++)
        {
            EdgeQueryPojo pe=new EdgeQueryPojo();
            String pv=paths[i];
            // System.out.println(pv+":");
            paths=input.split("/");
            String pattern1 = "^(\\d*),(\\d*)$";
            String pattern2 = "(\\w*)_(\\w*)$";
            String pattern3="^(\\d*)$";
            Pattern r1 = Pattern.compile(pattern1);
            Pattern r2 = Pattern.compile(pattern2);
            Pattern r3 = Pattern.compile(pattern3);
            if(r1.matcher(pv).find()&&i==0)
            {
                //System.out.println("nid");
                String page_slot[]=pv.split(",");
                PageId p=new PageId();
                p.pid=Integer.parseInt(page_slot[0]);
                NID nd=new NID();

                nd.pageNo=p;
                nd.slotNo=Integer.parseInt(page_slot[1]);
                pe.setNd(nd);
                pe.setWeight(-1);

            }
            else if(r2.matcher(pv).find())
            {
                //System.out.println("label");
                pe.setEdgelabel(pv);
                pe.setWeight(-1);

            }
            else if(r3.matcher(pv).find())
            {
                //System.out.println("weight");
                pe.setWeight(Integer.parseInt(pv));
            } else{
                //System.out.println("invalid");
            }
            list.add(pe);
        }

        return list;
    }

    public List<NodeQueryPojo> PathExpression1(String input)
    {
        String paths[]=new String[1000];
        List<NodeQueryPojo> list=new LinkedList<NodeQueryPojo>();
        paths=input.split("/");
        for(int i=0;i<paths.length;i++)
        {

            String pv=paths[i];
            //System.out.println(pv+":");
            NodeQueryPojo pe=new NodeQueryPojo();
            String pattern1 = "^(\\d*),(\\d*)$";
            String pattern2="^(\\w*)$";
            String pattern3="^(\\d*),(\\d*),(\\d*),(\\d*),(\\d*)$";
            Pattern r1 = Pattern.compile(pattern1);
            Pattern r2 = Pattern.compile(pattern2);
            Pattern r3 = Pattern.compile(pattern3);

            if(r1.matcher(pv).find()&&i==0)
            {
                //System.out.println("nid");
                String page_slot[]=pv.split(",");
                PageId p=new PageId();
                p.pid=Integer.parseInt(page_slot[0]);
                NID nd=new NID();

                nd.pageNo=p;
                nd.slotNo=Integer.parseInt(page_slot[1]);
                pe.setNd(nd);
                pe.setKey(1);
            }
            else if(r2.matcher(pv).find())
            {
                //System.out.println("label");
                pe.setLabel(pv);
                pe.setKey(2);
            }
            else if(r3.matcher(pv).find())
            {
                Descriptor desc=new Descriptor();
                String[] dsc=pv.split(",");
                desc.set(Integer.parseInt(dsc[0]), Integer.parseInt(dsc[1]), Integer.parseInt(dsc[2]), Integer.parseInt(dsc[3]), Integer.parseInt(dsc[4]));
                pe.setDesc(desc);
                pe.setKey(3);
            }
            else{
                //System.out.println("invalid");
            }
            list.add(pe);

        }


        return list;
    }
}
