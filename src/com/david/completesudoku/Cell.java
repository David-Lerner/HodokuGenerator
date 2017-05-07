package com.david.completesudoku;

/**
 *
 * @author David
 */
public interface Cell {

    boolean containsPossibility(int number);

    SubSudoku getColumn();

    int getId();

    boolean[] getPossibilities();

    int getPossibilityCount();

    SubSudoku getRow();

    SubSudoku getBox();

    int getValue();

    boolean isGiven();
    
    void setGiven(boolean given);

    boolean setPossibile(int number, boolean possible);

    void setValue(int value);

    void togglePossibile(int number);
    
    void removePossibilities();
    
    //for factory
    Cell createCell(int id, int length, int value, SubSudoku row, SubSudoku column, 
            SubSudoku box, boolean given);
    
}
