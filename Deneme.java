import java.util.ArrayList;

public class Deneme {
    public static void main(String [] args) {
        QuadTree t = new QuadTree(2, 0, 0, 4000, 4000);
        ArrayList<String> inters = t.getIntersections(2005, 3005, 100);

        for (String inter : inters)
            System.out.println(inter);
    }
}
