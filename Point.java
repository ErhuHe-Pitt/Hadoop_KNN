public class Point {
    private String id;
    private float x;
    private float y;

    public Point(String _id, float _x, float _y) {
        id = _id;
        x = _x;
        y = _y;
    }

    public String getId() {
        return id;
    }

    public float getX() {
        return x;
    }

    public float getY() {
        return y;
    }
}