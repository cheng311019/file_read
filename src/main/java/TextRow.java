public class TextRow{
    private Integer group;
    private Integer id;
    private float score;

    public TextRow(Integer group, Integer id, float score) {
        this.group = group;
        this.id = id;
        this.score = score;
    }

    public Integer getGroup() {
        return group;
    }

    public void setGroup(Integer group) {
        this.group = group;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return this.group + "," + this.id + "," +this.score;
    }
}
