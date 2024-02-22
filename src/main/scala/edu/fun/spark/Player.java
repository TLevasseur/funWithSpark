package edu.fun.spark;

public enum Player {
    BLACK("black"),
    WHITE("white");
    final String value;

    Player(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
