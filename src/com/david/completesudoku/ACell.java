package com.david.completesudoku;

import java.util.Arrays;

/**
 *
 * @author David
 */
public class ACell implements Cell {
    
    private int id;
    private int value;
    private boolean[] possibles;
    private int possibilityCount;
    private SubSudoku row;
    private SubSudoku column;
    private SubSudoku box;
    private boolean given;

    public ACell() {}; //as factory
    
    public ACell(int id, int length, SubSudoku row, SubSudoku column, 
            SubSudoku box) {
        this(id, length, 0, row, column, box, false);
    }

    public ACell(int id, int length, int value, SubSudoku row, SubSudoku column, 
            SubSudoku box, boolean given) {
        this.id = id;
        this.value = value;
        possibles = new boolean[length];
        possibilityCount = 0;
        this.row = row;
        this.column = column;
        this.box = box;
        this.given = given;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public SubSudoku getRow() {
        return row;
    }

    @Override
    public SubSudoku getColumn() {
        return column;
    }

    @Override
    public SubSudoku getBox() {
        return box;
    }

    @Override
    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public int getValue() {
        return value;
    }
    
    @Override
    public boolean isGiven() {
        return given;
    }

    @Override
    public void setGiven(boolean given) {
        this.given = given;
    }

    @Override
    public int getPossibilityCount() {
        return possibilityCount;
    }
    
    @Override
    public boolean containsPossibility(int number) {
        return possibles[number-1];
    }
    
    @Override
    public boolean[] getPossibilities() {
        return Arrays.copyOf(possibles, possibles.length);
    }
    
    @Override
    public boolean setPossibile(int number, boolean possible) {
        if (possible == possibles[number-1])
            return false;
        possibles[number-1] = possible;
        if (possibles[number-1])
            possibilityCount++;
        else
            possibilityCount--;
        return true;
    }
    
    @Override
    public void togglePossibile(int number) {
        if (possibles[number-1])
            possibilityCount--;
        else
            possibilityCount++;
        possibles[number-1] = !possibles[number-1];
    }

    @Override
    public void removePossibilities() {
        possibles = new boolean[possibles.length];
        possibilityCount = 0;
    }
    
    @Override
    public Cell createCell(int id, int length, int value, SubSudoku row, SubSudoku column, SubSudoku box, boolean given) {
        return new ACell(id, length, value, row, column, box, given);
    }
}
