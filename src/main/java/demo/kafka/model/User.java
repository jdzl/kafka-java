package demo.kafka.model;

public class User {
    private int id;
    private String name;
    private String lastName;
    private Boolean active;

    public User() {
    }

    public User(int id, String name, String lastName, Boolean active) {
        this.id = id;
        this.name = name;
        this.lastName = lastName;
        this.active = active;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }
}
