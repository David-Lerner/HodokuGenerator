package com.david.completesudoku;

/**
 *
 * @author David
 */
public class Sudoku {
    
    private int length;
    private Cell[][] cells;
    private SubSudoku[] rows;
    private SubSudoku[] columns;
    private SubSudoku[] boxes;
    
    /**
     * Create a new Sudoku with the default base of 3
     */
    public Sudoku() {
        this(3);
    }

    /**
     * Create a new empty Sudoku with a given base
     * @param base the base the of the Sudoku
     */
    public Sudoku(int base) {
        length = base*base;
        cells = new Cell[length][length];
        rows = new SubSudoku[length];
        columns = new SubSudoku[length];
        boxes = new SubSudoku[length];
        for (int i = 0; i < length; i++) {
            rows[i] = new SubSudoku(length);
            columns[i] = new SubSudoku(length);
            boxes[i] = new SubSudoku(length);
        }
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                SubSudoku row = rows[i];
                SubSudoku column = columns[j];
                SubSudoku box = boxes[(j/base+(i/base)*base)];
                Cell cell = new ACell(j+i*length, length, row, column, box);
                cells[i][j] = cell;
                row.addcell(cell);
                column.addcell(cell);
                box.addcell(cell);
            }
        }
    }
      
    /**
     * Create a Sudoku based on a given configuration
     * @param given a given n x n Sudoku with empty cells as 0
     */
    public Sudoku (int[][] given) {
        this(given, new ACell());
    }
    
    public Sudoku (int[][] given, Cell factory) {
        length = given.length;
        cells = new Cell[length][length];
        rows = new SubSudoku[length];
        columns = new SubSudoku[length];
        boxes = new SubSudoku[length];
        for (int i = 0; i < length; i++) {
            rows[i] = new SubSudoku(length);
            columns[i] = new SubSudoku(length);
            boxes[i] = new SubSudoku(length);
        }
        int base = (int)Math.sqrt(length);
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                SubSudoku row = rows[i];
                SubSudoku column = columns[j];
                SubSudoku box = boxes[(j/base+(i/base)*base)];
                Cell cell = factory.createCell(j+i*length, length, given[i][j], row, column, box, given[i][j]!=0);
                cells[i][j] = cell;
                row.addcell(cell);
                column.addcell(cell);
                box.addcell(cell);
            }
        }
    }
    
    /**
     * Create a Sudoku that is a copy of another
     * @param sudoku the sudoku you want copied
     */
    public Sudoku (Sudoku sudoku) {
        length = sudoku.getLength();
        cells = new Cell[length][length];
        rows = new SubSudoku[length];
        columns = new SubSudoku[length];
        boxes = new SubSudoku[length];
        for (int i = 0; i < length; i++) {
            rows[i] = new SubSudoku(length);
            columns[i] = new SubSudoku(length);
            boxes[i] = new SubSudoku(length);
        }
        int base = (int)Math.sqrt(length);
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                SubSudoku row = rows[i];
                SubSudoku column = columns[j];
                SubSudoku box = boxes[(j/base+(i/base)*base)];
                Cell oldCell = sudoku.getCell(i, j);
                Cell newCell = oldCell.createCell(oldCell.getId(), length, oldCell.getValue(), row, column, box, oldCell.isGiven());
                if (oldCell.getValue() == 0) {
                    for (int n = 1; n <= length; n++) {
                        if (oldCell.containsPossibility(n))
                            newCell.setPossibile(n, true);
                    }
                }
                cells[i][j] = newCell;
                row.addcell(newCell);
                column.addcell(newCell);
                box.addcell(newCell);
            }
        }
    }
    
    /**
     * Create a Sudoku from a 3 data arrays
     * @param value the values of the cells
     * @param given whether the cells are given
     * @param possible the possibilities of the cells
     */
    public Sudoku (int[] value, boolean[] given, boolean[][] possible) {
        length = (int)Math.sqrt(value.length);
        cells = new Cell[length][length];
        rows = new SubSudoku[length];
        columns = new SubSudoku[length];
        boxes = new SubSudoku[length];
        for (int i = 0; i < length; i++) {
            rows[i] = new SubSudoku(length);
            columns[i] = new SubSudoku(length);
            boxes[i] = new SubSudoku(length);
        }
        int base = (int)Math.sqrt(length);
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                SubSudoku row = rows[i];
                SubSudoku column = columns[j];
                SubSudoku box = boxes[(j/base+(i/base)*base)];
                int id = j+i*length;
                Cell cell = new ACell(id, length, value[id], row, column, box, given[id]);
                if (cell.getValue() == 0) {
                    for (int n = 1; n <= length; n++) {
                        if (possible[id][n-1])
                            cell.setPossibile(n, true);
                    }
                }
                cells[i][j] = cell;
                row.addcell(cell);
                column.addcell(cell);
                box.addcell(cell);
            }
        }
    }

    public int getLength() {
        return length;
    }
    
    public Cell getCell(int i, int j) {
        return cells[i][j];
    }
    
    public SubSudoku getRow(int i) {
        return rows[i];
    }
    
    public SubSudoku getColumn(int i) {
        return columns[i];
    }
    
    public SubSudoku getBox(int i) {
        return boxes[i];
    }
}
