import java.util.ArrayList;
import java.util.HashMap;

public class QuadTree {
    public int N;          // Tree Level
    public Cell root;      // Tree root
    public HashMap<String, Cell> cellMap;

    public QuadTree (int _N, float x1, float y1, float x2, float y2) {
        cellMap = new HashMap<String, Cell>();
        N =_N;
        root = new Cell("A", x1, y1, x2, y2);
        cellMap.put("A", root);
        CreateTree(root);
    }

    public void CreateTree(Cell cell) {
        if (cell.getId().length() < N+1) {
            float mid_x = (cell.getX2() - cell.getX1())/2 + cell.getX1();
            float mid_y = (cell.getY2() - cell.getY1())/2 + cell.getY1();

            Cell c0 = new Cell(cell.getId()+"0", cell.getX1(), cell.getY1(), mid_x, mid_y);
            Cell c1 = new Cell(cell.getId()+"1", mid_x, cell.getY1(), cell.getX2(), mid_y);
            Cell c2 = new Cell(cell.getId()+"2", cell.getX1(), mid_y, mid_x, cell.getY2());
            Cell c3 = new Cell(cell.getId()+"3", mid_x, mid_y, cell.getX2(), cell.getY2());

            cellMap.put(cell.getId()+"0", c0);
            cellMap.put(cell.getId()+"1", c1);
            cellMap.put(cell.getId()+"2", c2);
            cellMap.put(cell.getId()+"3", c3);

            cell.addChild(c0);
            cell.addChild(c1);
            cell.addChild(c2);
            cell.addChild(c3);

            CreateTree(c0);
            CreateTree(c1);
            CreateTree(c2);
            CreateTree(c3);
        }
    }

    public String findId(Cell c, Point p) {
        if(c.isChildrenEmpty()) {
            return c.getId();
        }

        float mid_x = (c.getX2()-c.getX1())/2 + c.getX1();
        float mid_y = (c.getY2()-c.getY1())/2 + c.getY1();

        if( p.getX()<mid_x && p.getY()<mid_y)
            return findId(c.getChildren().get(0), p);
        else if( p.getX()>= mid_x && p.getY()< mid_y)
            return findId(c.getChildren().get(1), p);
        else if( p.getX()<mid_x && p.getY()>=mid_y)
            return findId(c.getChildren().get(2), p);
        return findId(c.getChildren().get(3), p);
    }

    public void addPoints(String id, int numPoints) {
        Cell temp = root;
        for (int i = 1; i < id.length(); i++)
            temp = temp.getChildren().get(Integer.parseInt(id.charAt(i) + ""));
        temp.addNumber(numPoints);
    }

    public void merge(int K) {
        int total = mergeHelper(root, K);
        System.out.println("Total: " + total);
    }

    private int mergeHelper(Cell c, int K) {
        if (c.isChildrenEmpty()) return c.getNum();

        int c0 = mergeHelper(c.getChildren().get(0), K);
        int c1 = mergeHelper(c.getChildren().get(1), K);
        int c2 = mergeHelper(c.getChildren().get(2), K);
        int c3 = mergeHelper(c.getChildren().get(3), K);

        if (c0 < K || c1 < K || c2 < K || c3 < K) {
            // System.out.println(c.getID() + " merged childrens. " + c0 + ", " + c1 + ", " + c2 + ", " + c3);
            c.deleteChildren();
            c.addNumber(c0 + c1 + c2 + c3);
        }

        return c0 + c1 + c2 + c3;
    }

    public Cell getCell(String _id) {
        return cellMap.get(_id);
    }

    public ArrayList<String> getIntersections(float _x, float _y, float _r) {
        ArrayList<String> keke = new ArrayList<String>();
        intersect(root, _x, _y, _r, keke);
        return keke;
    }

    private void intersect(Cell c, float x, float y, float r, ArrayList<String> l) {
        float width = c.getX2() - c.getX1();
        float height = c.getY2() - c.getY1();

        float distX = Math.abs(x - c.getX1() - (width / 2));
        float distY = Math.abs(y - c.getY1() - (height / 2));

        if (distX > (width / 2 + r) || distY > (height / 2 + r)) return;

        float dx = distX - (width / 2);
        float dy = distY - (height / 2);

        boolean check = (Math.pow(dx, 2) + Math.pow(dy, 2)) <= Math.pow(r, 2);

        if (distX <= (width / 2) || distY <= (height / 2) || check) {

            if (c.isChildrenEmpty()) {
                l.add(c.getId());
            } else {
                intersect(c.getChildren().get(0), x, y, r, l);
                intersect(c.getChildren().get(1), x, y, r, l);
                intersect(c.getChildren().get(2), x, y, r, l);
                intersect(c.getChildren().get(3), x, y, r, l);
            }
        }
    }
}
