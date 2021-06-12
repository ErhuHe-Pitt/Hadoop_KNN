import java.util.ArrayList;

public class Cell {
    private String id;
    private float x1;
    private float y1;
    private float x2;
    private float y2;
    private int num;
    private ArrayList<Cell> children;

    public Cell (String _id, float _x1, float _y1, float _x2, float _y2) {
        id = _id;
        x1 = _x1;
        y1 = _y1;
        x2 = _x2;
        y2 = _y2;
        num = 0;
        children = new ArrayList<Cell>();
    }

    public String getId() {
        return id;
    }

    public float getX1() {
        return x1;
    }

    public float getY1() {
        return y1;
    }

    public float getX2() {
        return x2;
    }

    public float getY2() {
        return y2;
    }

    public void addChild(Cell c) {
        children.add(c);
    }

    public boolean isChildrenEmpty() {
        return children.size() == 0;
    }

    public ArrayList<Cell> getChildren(){
        return children;
    }

    public void addNumber(int _num) {
        num = _num;
    }

    public int getNum() {
        return num;
    }

    public void deleteChildren() {
        children = new ArrayList<Cell>();
    }

    public String getID() {
        return id;
    }
}