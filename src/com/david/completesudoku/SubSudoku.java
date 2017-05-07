package com.david.completesudoku;

/**
 *
 * @author David
 */
public class SubSudoku {
    
    private Cell[] cells;
    private int length;
    private int cellcount;

    public SubSudoku(int length) {
        this.length = length;
        cells = new Cell[length];
        cellcount = 0;
    }
    
    public Cell getCell(int i) {
        return cells[i];
    }
    
    public boolean addcell(Cell cell) {
        if (cellcount >= length)
            return false;
        cells[cellcount++] = cell;
        return true;
    }
}
