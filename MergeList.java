import java.util.ArrayList;
import java.util.HashSet;

public class MergeList {
    public static String merge(String list1, String list2, int K) {
        String [] tok1 = list1.split(",");
        String [] tok2 = list2.split(",");
        HashSet<String> added = new HashSet<String>();

        ArrayList<PDist> l1 = new ArrayList<PDist>();
        ArrayList<PDist> l2 = new ArrayList<PDist>();
        ArrayList<PDist> result = new ArrayList<PDist>();

        for (int i = 0; i < tok1.length; i = i + 2) {
            l1.add(new PDist(tok1[i], Float.parseFloat(tok1[i + 1])));
            l2.add(new PDist(tok2[i], Float.parseFloat(tok2[i + 1])));
        }

        int j1 = 0, j2 = 0;

        while (result.size() < K) {
            if (j1 >= K) {
                if (!added.contains(l2.get(j2).id)) {
                    result.add(l2.get(j2));
                    added.add(l2.get(j2).id);
                }
                j2++;
            }
            else if (j2 >= K) {
                if (!added.contains(l1.get(j1).id)) {
                    result.add(l1.get(j1));
                    added.add(l1.get(j1).id);
                }
                j1++;
            }
            else {
                if (l1.get(j1).dist < l2.get(j2).dist) {
                    if (!added.contains(l1.get(j1).id)) {
                        result.add(l1.get(j1));
                        added.add(l1.get(j1).id);
                    }
                    j1++;
                }
                else {
                    if (!added.contains(l2.get(j2).id)) {
                        result.add(l2.get(j2));
                        added.add(l2.get(j2).id);
                    }
                    j2++;
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        String prefix = "";

        for (int i = 0; i < K; i++) {
            sb.append(prefix);
            prefix = ",";
            sb.append(result.get(i).id + prefix + String.valueOf(result.get(i).dist));
        }

        return sb.toString();
    }
}

class PDist {
    String id;
    float dist;

    public PDist(String _id, float _dist) {
        id = _id;
        dist = _dist;
    }
}
