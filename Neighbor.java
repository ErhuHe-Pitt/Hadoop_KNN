import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

public class Neighbor {
    private Point element;
    private ArrayList<Point> pl;
    private PriorityQueue<PointInfo> q;
    private int K;

    public Neighbor(Point _element, ArrayList<Point> _pl, int _K) {
        element = _element;
        pl = new ArrayList<Point>();
        pl.addAll(_pl);
        K = _K;
        q = new PriorityQueue<PointInfo>(K, new Comparator<PointInfo>() {
            @Override
            public int compare(PointInfo o1, PointInfo o2) {
                if (o1.distance > o2.distance)
                    return 1;
                else if (o1.distance < o2.distance)
                    return -1;
                else
                    return 0;
            }
        });
    }

    public float getDistance(Point p1, Point p2) {
        return (float) Math.sqrt(Math.pow((p1.getX() - p2.getX()),2) + Math.pow((p1.getY() - p2.getY()),2));
    }

    public String DistanceList () {
        for (Point p : pl) {
            if (element.getId().equals(p.getId())) {
                continue;
            } else {
                float d = getDistance(element, p);
                PointInfo temp = new PointInfo(p, d);
                if (q.size() < K) {
                    q.add(temp);
                } else if (q.peek().distance > d) {
                    q.poll();
                    q.add(temp);
                }
            }
        }

        ArrayList<String> ret = new ArrayList<String>();
        while(!q.isEmpty()) {
            PointInfo temp =q.poll();
            ret.add(temp.p.getId() + "," + temp.distance);
        }
        return String.join(",", ret);
    }
}

class PointInfo {
    public Point p;
    public float distance;
    public PointInfo (Point _p, float _distance) {
        p = _p;
        distance = _distance;
    }
}