package tests;

import component.operator.in1.aggregate.TimeAggregate;

import java.util.List;

public class AggregateWindowTests {

    // Character | to separate tests, each test 4 values: WS,WA,ts,window starts (separated with /)
    private static final String tests = "1,1,15,15|10,10,12345,12340|1234,1234,0,0|2,1,20,19/20|2,2,50,50|3,1,60,58/59/60|3,2,100,98/100|3,3,1111,1110|10,2,1009,1000/1002/1004/1006/1008|10,5,1009,1000/1005|10,8,117,112|10,9,90,81/90|10,8,89,80/88|10,9,1234,1233|10,8,17,8/16|10,9,18,9/18|10,8,31,24|10,9,1119991,1119987";

    public static void main(String[] args) {

        for(String test : tests.split("\\|")) {
            String[] tokens = test.split(",");
            long ws = Long.valueOf(tokens[0]);
            long wa = Long.valueOf(tokens[1]);
            long ts = Long.valueOf(tokens[2]);
            TimeAggregate ta = new TimeAggregate("x",0,1,ws,wa,null,null) {
                @Override
                public List processTupleIn1(Object tuple) {
                    return null;
                }
            };
            long winStart = ta.getEarliestWinStartTS(ts);
            long expectedWinStart = Long.valueOf(tokens[3].split("/")[0]);

            System.out.println("Aggregate ws "+ws+" wa "+wa+" ts "+ts+" start ts: "+winStart + " ("+ta.getContributingWindows(ts)+ " wins) expected: "+expectedWinStart + " ("+tokens[3].split("/").length+" wins)");
            assert(winStart==expectedWinStart && ta.getContributingWindows(ts)==tokens[3].split("/").length);
        }

    }

}
