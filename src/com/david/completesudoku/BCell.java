package com.david.completesudoku;

import java.util.BitSet;

/**
 *
 * @author David
 */
public class BCell implements Cell{
    
    private int id;
    private int value;
    private BitSet possibles;
    private SubSudoku row;
    private SubSudoku column;
    private SubSudoku box;
    private boolean given;
    
    public BCell() {}; //as factory

    public BCell(int id, int length, SubSudoku row, SubSudoku column, 
            SubSudoku box) {
        this(id, length, 0, row, column, box, false);
    }

    public BCell(int id, int length, int value, SubSudoku row, SubSudoku column, 
            SubSudoku box, boolean given) {
        this.id = id;
        this.value = value;
        possibles = new BitSet(length);
        this.row = row;
        this.column = column;
        this.box = box;
        this.given = given;
    }

    public int getId() {
        return id;
    }

    public SubSudoku getRow() {
        return row;
    }

    public SubSudoku getColumn() {
        return column;
    }

    public SubSudoku getBox() {
        return box;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
    
    public boolean isGiven() {
        return given;
    }

    public void setGiven(boolean given) {
        this.given = given;
    }
    
    public int getPossibilityCount() {
        return possibles.cardinality();
    }
    
    public boolean containsPossibility(int number) {
        return possibles.get(number-1);
    }
    
    public boolean[] getPossibilities() {
        boolean[] bArray = new boolean[possibles.length()];
        for (int i = 0; i < possibles.length(); i++)  
            bArray[i] = possibles.get(i);
        return bArray;
    }
    
    public boolean setPossibile(int number, boolean possible) {        
        if (possibles.get(number-1) == possible)
            return false;
        possibles.set(number-1, possible);
        return true;
    }
    
    public void togglePossibile(int number) {
        possibles.flip(number-1);
    }

    @Override
    public void removePossibilities() {
        possibles = new BitSet(possibles.length());
    }

    @Override
    public Cell createCell(int id, int length, int value, SubSudoku row, SubSudoku column, SubSudoku box, boolean given) {
        return new BCell(id, length, value, row, column, box, given);
    }


}
