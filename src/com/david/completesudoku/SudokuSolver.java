package com.david.completesudoku;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Stack;

/**
 *
 * @author David
 */
public class SudokuSolver {
    
    public static Sudoku solve(Sudoku s) {
        int length = s.getLength();
        int[][] cells = new int[length][length];
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                cells[i][j] = s.getCell(i, j).getValue();
            }
        }
        if (solve(0, 0, cells))
            return new Sudoku(cells);
        else
            return null;
    }
    
    public static boolean solve(int i, int j, int[][] cells) {
        if (i == 9) {
            i = 0;
            if (++j == 9)
                return true;
        }
        if (cells[i][j] != 0)  // skip filled cells
            return solve(i+1,j,cells);

        for (int val = 1; val <= 9; ++val) {
            if (legal(i,j,val,cells)) {
                cells[i][j] = val;
                if (solve(i+1,j,cells))
                    return true;
            }
        }
        cells[i][j] = 0; // reset on backtrack
        return false;
    }

    public static boolean legal(int i, int j, int val, int[][] cells) {
        for (int k = 0; k < 9; ++k)  // row
            if (val == cells[k][j])
                return false;

        for (int k = 0; k < 9; ++k) // col
            if (val == cells[i][k])
                return false;

        int boxRowOffset = (i / 3)*3;
        int boxColOffset = (j / 3)*3;
        for (int k = 0; k < 3; ++k) // box
            for (int m = 0; m < 3; ++m)
                if (val == cells[boxRowOffset+k][boxColOffset+m])
                    return false;

        return true; // no violations, so it's legal
    }

    public static final int STRATEGY_NUMBER = 10;
    private Sudoku sudoku;
    private Sudoku original;
    private int length;
    private int remaining;
    private int strategy;
    private int[] strategyCounts;
    private int[] cellStrategies;
    private Stack<Sudoku> sudokuStack;
    private Stack<Integer> remainingStack;
    private Stack<int[]> strategyCountStack;
    private Stack<int[]> cellStrategyStack;
    
    public SudokuSolver(Sudoku s) {
        sudoku = s;
        original = s;
        length = s.getLength();
        remaining = length*length;
        //8 strategies + no strategy (given)
        strategy = 0;
        strategyCounts = new int[STRATEGY_NUMBER];
        cellStrategies = new int[length*length];
        sudokuStack = new Stack<>();
        remainingStack = new Stack<>();
        strategyCountStack = new Stack<>();
        cellStrategyStack = new Stack<>();
    }

    public Sudoku getSudoku() {
        return sudoku;
    }
    
    /*
    Strategy index:
    0: None
    1: Possible Values
    2: Hidden Singles
    3: Naked Pairs
    4: Naked Triples
    5: Hidden Pairs
    6: Hidden Triples
    7: Intersection Removal
    8: Brute Force
    */
    private void setCellStrategy(Cell c, int strategy) {
        if (strategy > cellStrategies[c.getId()])
            cellStrategies[c.getId()] = strategy;
    }
    
    public static String getStrategyName(int index) {
        String[] strategyNames = {
            "None", 
            "Possible Values",
            "Hidden Singles",
            "Naked Pairs",
            "Naked Triples",
            "Hidden Pairs",
            "Hidden Triples",
            "Intersection Removal",
            "X Wing",
            "Brute Force"};
        if (index < strategyNames.length && index >= 0) {
            return strategyNames[index];
        }
        return "None";
    }
    
    public int getStrategyCount(int index) {
        if (index < strategyCounts.length &&  index >= 0)
            return strategyCounts[index];
        return 0;
    }
    
    public int getStrategyCountByCell(int index) {
        int[] byCell = new int[STRATEGY_NUMBER];
        for (int i = 0; i < cellStrategies.length; i++) {
            byCell[cellStrategies[i]]++;
        }
        if (index < byCell.length && index >= 0)
            return byCell[index];
        return 0;
    }
    
    public boolean isStrategyUsed(int index) {
        if (index < strategyCounts.length && index >= 0)
            return (strategyCounts[index] > 0 || index == 0);
        return false;
    }
    
    private void printStrategies() {  
        for (int i = 0; i < STRATEGY_NUMBER; i++) {
            System.out.printf("%-21s %3d By Cell: %d%n", 
                    getStrategyName(i)+":", getStrategyCount(i), getStrategyCountByCell(i));
        }
    }
   
    public boolean solve() {
        initializeCells();
        return recursiveSolve();
    }
    
    public boolean recursiveSolve() {
        //System.out.println("Before:");
        //writeMatrix();
        int changed = 1;
        while (changed > 0) {
            if (update() < 0)
                return false;
            if (remaining == 0) {
                break;
            }
            changed = findHiddenSingles();
            //System.out.println("Hidden Singles: "+changed);
            if (changed > 0)
                continue;
            changed = findNakedPairs();
            //System.out.println("Naked Pairs: "+changed);
            if (changed > 0)
                continue;
            changed = findNakedTriples();
            //System.out.println("Naked Triples: "+changed);
            if (changed > 0)
                continue;
            changed = findHiddenPairs();
            //System.out.println("Hidden Pairs: "+changed);
            if (changed > 0)
                continue;
            changed = findHiddenTriples();
            //System.out.println("Hidden Triples: "+changed);
            if (changed > 0)
                continue;
            changed = intersectionRemoval();
            //System.out.println("Intersection Reduction: "+changed);
            if (changed > 0)
                continue;
            changed = findXWing();
            //System.out.println("Intersection Reduction: "+changed);
        }
        
        if (remaining > 0) {
            return bruteForce();
        }
        //printStrategies();
        /*if (!checkValidity(SudokuSolver.solve(sudoku))) {
            System.out.print("ERROR!"+System.lineSeparator());
        }*/
        //System.out.println("After:");
        //writeMatrix();
        updateOriginal();
        return checkValidity(sudoku);
    }
    
    public void initializeCells() {
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                if (sudoku.getCell(i, j).isGiven()) {
                    remaining--;
                } else {
                    sudoku.getCell(i, j).setValue(0);
                    for (int n = 1; n <= length; ++n)
                        sudoku.getCell(i, j).setPossibile(n, true);
                }
            }
        }
    }
    
    private int update() {
        int solved = 1;
        strategy = 1;
        while (solved > 0) {
            solved = findPossibleValues();
        }
        strategy++;
        return solved;
    }
    
    private int findPossibleValues() {
        int solved = 0;
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                Cell c = sudoku.getCell(i, j);
                if (c.getValue() != 0)
                    continue;
                changes += setPossibleValues(c);
                if (c.getPossibilityCount() == 0) {
                    return -1;
                } else if (c.getPossibilityCount() == 1) {
                    //found a number
                    ++solved;
                    --remaining;
                    int n = 1;
                    while (!c.containsPossibility(n)) {
                        ++n;
                    }
                    c.setValue(n);
                    c.setPossibile(n, false);
                }
            }
        }
        //System.out.println("Possible Values: "+changes);
        strategyCounts[strategy] += changes;
        return solved;
    }
    
    public int setPossibleValues(Cell c) {
        int changes = 0;
        int preset = c.getPossibilityCount();
        //remove all possibilities that are already given in the cell's box, row, and column
        for (int i = 0; i < length; ++i) {
            if (c.getBox().getCell(i).getValue() != 0) {
                c.setPossibile(c.getBox().getCell(i).getValue(), false);
            }
            if (c.getColumn().getCell(i).getValue() != 0) {
                c.setPossibile(c.getColumn().getCell(i).getValue(), false);
            }
            if (c.getRow().getCell(i).getValue() != 0) {
                c.setPossibile(c.getRow().getCell(i).getValue(), false);
            }
        }
        if (c.getPossibilityCount() < preset) {
            //possibilities were removed
            //System.out.println("Reduction: possible value");
            setCellStrategy(c, strategy);
            ++changes;
        }
        return changes;
    }
    
    private int findHiddenSingles() {
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            changes += setHiddenSingles(sudoku.getBox(i));
            changes += setHiddenSingles(sudoku.getRow(i));
            changes += setHiddenSingles(sudoku.getColumn(i));
        }
        strategyCounts[strategy++] += changes;
        return changes;
    }
    
    private int setHiddenSingles(SubSudoku s) {
        int changes = 0;
        for (int n = 1; n <= length; ++n) {
            int index = 0, count = 0;
            for (int i = 0; i < length; ++i) {
                if (s.getCell(i).containsPossibility(n)) {
                    ++count;
                    if (count > 1) 
                        break;
                    index = i;
                    //update();
                }
            }
            if (count == 1) {
                //found a number
                Cell c = s.getCell(index);
                //System.out.println("Reduction: hidden single");
                ++changes;
                /*s.getCell(index).setValue(n);*/
                for (int i = 1; i <= length; ++i)
                    c.setPossibile(i, false);
                c.setPossibile(n, true);
                setCellStrategy(c, strategy);
            }
        }
        return changes;
    }
    
    private int findNakedPairs() {
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            changes += setNakedPairs(sudoku.getBox(i));
            changes += setNakedPairs(sudoku.getRow(i));
            changes += setNakedPairs(sudoku.getColumn(i));
        }
        strategyCounts[strategy++] += changes;
        return changes;
    }
    
    private int setNakedPairs(SubSudoku s) {
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            Cell c = s.getCell(i);
            //check if cell contains a pair
            if (c.getPossibilityCount() != 2)
                continue;
            //pair is a, b
            int n = 1;
            while (!c.containsPossibility(n))
                ++n;
            int a = n++;
            while (!c.containsPossibility(n))
                ++n;
            int b = n;
            //check if other cells contain the pair a, b
            for (int j = i+1; j < length; ++j) {
                Cell other = s.getCell(j);
                if (other.getPossibilityCount() == 2 && other.containsPossibility(a) && other.containsPossibility(b)) {
                    //if a match is found, remove the pair from all other cells
                    int cellsModified = 0;
                    for (int k = 0; k < length; k++) {
                        Cell temp = s.getCell(k);         
                        if (temp.getValue() == 0 && j != k && temp.getId() != c.getId()) {
                            //increment cells modified count only if possibilty(s) were set                            
                            if (temp.setPossibile(a, false) | temp.setPossibile(b, false)) {
                                //System.out.println("Reduction: naked pair");
                                ++cellsModified;
                                setCellStrategy(temp, strategy);
                            }
                        }
                    }
                    //increment change count only if cells were modified
                    if (cellsModified > 0) {
                        ++changes;
                    }
                    break;
                }
            }
        }
        return changes;
    }
    
    private int findNakedTriples() {
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            changes += setNakedTriples(sudoku.getBox(i));
            changes += setNakedTriples(sudoku.getRow(i));
            changes += setNakedTriples(sudoku.getColumn(i));
        }
        strategyCounts[strategy++] += changes;
        return changes;
    }
    
    private int setNakedTriples(SubSudoku s) {
        int changes = 0;
        //find number of cells with 2-3 cells
        int setSize = 0;
        for (int i = 0; i < length; ++i) {
            int count = s.getCell(i).getPossibilityCount();
            if (count == 2 || count == 3) {
                setSize++;
            }
        }
        //return if < 3 cells with 2-3 cells
        if (setSize < 3) {
            return changes;
        }
        
        //create/fill set data structures for finding triples
        int[] indexes = new int[setSize];
        BitSet[] possibilities = new BitSet[setSize];
        int index = 0;
        for (int i = 0; i < setSize; ++i) {
            Cell c = s.getCell(index);
            while (c.getPossibilityCount() != 3 && c.getPossibilityCount() != 2 ) {
                c = s.getCell(++index);
            }
            //add index to triples set
            indexes[i] = index++;
            //create bitset representing possibilities
            possibilities[i] = new BitSet(length);
            for (int n = 1; n <= length; n++) {
                if (c.containsPossibility(n)) {
                    possibilities[i].set(n-1);
                }
            }
        }
        //check if the union of all combinations of three cells forms a set of size 3 
        for (int i = 0; i < setSize-2; ++i) {
            for (int j = i+1; j < setSize-1; ++j) {
                for (int k = j+1; k < setSize; ++k) {
                    BitSet union = possibilities[i].get(0, length);
                    union.or(possibilities[j]);
                    union.or(possibilities[k]);
                    if (union.cardinality() == 3) {
                        //triple found
                        int a = union.nextSetBit(0)+1;
                        int b = union.nextSetBit(a)+1;
                        int c = union.nextSetBit(b)+1;
                        //remove possibilities from other cells
                        int cellsModified = 0;
                        for (int n = 0; n < length; n++) {
                            Cell temp = s.getCell(n);
                            if (temp.getValue() == 0 && n != indexes[i] && n != indexes[j] && n != indexes[k]) {
                                //increment cells modified count only if possibilty(s) were set
                                //System.out.println("Reduction: naked triple");
                                if (temp.setPossibile(a, false) | temp.setPossibile(b, false) | temp.setPossibile(c, false)) {
                                    //System.out.println("Reduction: naked triple");
                                    ++cellsModified;
                                    setCellStrategy(temp, strategy);
                                }
                            }
                        }
                        //increment change count only if cells were modified
                        if (cellsModified > 0) {
                            ++changes;
                        }
                    }
                }
            }
        }

        return changes;
    }
    
    private int findHiddenPairs() {
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            changes += setHiddenPairs(sudoku.getBox(i));
            changes += setHiddenPairs(sudoku.getRow(i));
            changes += setHiddenPairs(sudoku.getColumn(i));
        }
        strategyCounts[strategy++] += changes;
        return changes;
    }
    
    private int setHiddenPairs(SubSudoku s) {
        int changes = 0;
        int[] pairs = new int[length];
        //find the indexes of hidden pairs
        for (int n = 1; n <= length; ++n) {
            int count = 0;
            int ids = 0;
            OUTER:
            for (int i = 0; i < length; ++i) {
                if (s.getCell(i).containsPossibility(n)) {
                    ++count;
                    //encode pair cell indexes as single int
                    switch (count) {
                        case 1:
                            ids += i*length;
                            break;
                        case 2:
                            ids += i;
                            break;
                        default:
                            break OUTER;
                    }
                }
            }
            pairs[n-1] = (count == 2) ? ids : -1;
        }
        for (int i = 0; i < pairs.length-1; i++) {
            if (pairs[i] > 0) {
                for (int j = i+1; j < pairs.length; j++) {
                    if (pairs[i] == pairs[j]) {
                        //hidden pair found
                        int cellsModified = 0;
                        Cell a = s.getCell(pairs[i] / length);
                        Cell b = s.getCell(pairs[i] % length);                 
                        //remove other possibilities from cells a & b
                        if (a.getPossibilityCount() > 2) {
                            //increment cells modified count only if cell was hidden
                            cellsModified++;
                            a.removePossibilities();
                            a.setPossibile(i+1, true);
                            a.setPossibile(j+1, true);
                            //System.out.println("Reduction: hidden pair");
                            setCellStrategy(a, strategy);
                        }
                        if (b.getPossibilityCount() > 2) {
                            //increment cells modified count only if cell was hidden
                            cellsModified++;
                            b.removePossibilities();
                            b.setPossibile(i+1, true);
                            b.setPossibile(j+1, true);
                            //System.out.println("Reduction: hidden pair");
                            setCellStrategy(b, strategy);
                        }
                        //increment change count only if cells were modified
                        if (cellsModified > 0) {                      
                            ++changes;
                        }
                        break;
                    }
                }
            }
        }
        
        return changes;
    }
    
    private int findHiddenTriples() {
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            changes += setHiddenTriples(sudoku.getBox(i));
            changes += setHiddenTriples(sudoku.getRow(i));
            changes += setHiddenTriples(sudoku.getColumn(i));
        }
        strategyCounts[strategy++] += changes;
        return changes;
    }
    
    private int setHiddenTriples(SubSudoku s) {
        int changes = 0;
        
        BitSet indexes[] = new BitSet[length];
        int[] counts = new int[length];
        //find the the counts and index bitset of each number
        for (int n = 1; n <= length; ++n) {
            int count = 0;
            indexes[n-1] = new BitSet(length);
            for (int i = 0; i < length; ++i) {
                if (s.getCell(i).containsPossibility(n)) {
                    ++count;
                    //save the index to corresponding bitset
                    if (count <= 3) {
                        indexes[n-1].set(i);
                    } else {
                        break;
                    }
                }
            }
            //save count if = 2 or 3
            counts[n-1] = (count == 2 || count == 3) ? count : -1;
        }
        
        //check if the union of all combinations of three numbers that appear 2-3 times forms a set of size 3 
        for (int i = 0; i < length-2; ++i) {
            if (counts[i] < 2)
                continue;
            for (int j = i+1; j < length-1; ++j) {
                if (counts[j] < 2)
                    continue;
                for (int k = j+1; k < length; ++k) {
                    if (counts[k] < 2)
                        continue;
                    BitSet union = indexes[i].get(0, length);
                    union.or(indexes[j]);
                    union.or(indexes[k]);
                    if (union.cardinality() == 3) {
                        //hidden triple found
                        int cellsModified = 0;
                        int a = union.nextSetBit(0);
                        int b = union.nextSetBit(a);
                        int c = union.nextSetBit(b);
                        //remove other possibilities from cells a, b, and c
                        int aChanged = 0;
                        int bChanged = 0;
                        int cChanged = 0;
                        for (int n = 1; n <= length; n++) {
                            if (n != i+1 && n != j+1 && n != k+1) {
                                //set all other possibilities to false
                                if (s.getCell(a).setPossibile(n, false)) {
                                    aChanged++;
                                }
                                if (s.getCell(b).setPossibile(n, false)) {
                                    bChanged++;
                                }
                                if (s.getCell(c).setPossibile(n, false)) {
                                    cChanged++;
                                }
                            }
                        }
                        //increment cells modified count only if the respective cells were modified
                        if (aChanged > 0) {
                            cellsModified++;
                            //System.out.println("Reduction: hidden triple");
                            setCellStrategy(s.getCell(a), strategy);
                        }
                        if (bChanged > 0) {
                            cellsModified++;
                            //System.out.println("Reduction: hidden triple");
                            setCellStrategy(s.getCell(b), strategy);
                        }
                        if (cChanged > 0) {
                            cellsModified++;
                            //System.out.println("Reduction: hidden triple");
                            setCellStrategy(s.getCell(c), strategy);
                        }
                        
                        //increment change count only if cells were modified
                        if (cellsModified > 0) {                      
                            ++changes;
                        }
                    }
                }
            }
        }
        
        return changes;
    }
    
    private int intersectionRemoval() {
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            changes += pointingNumbers(sudoku.getBox(i));
            changes += boxLine(sudoku.getRow(i));
            changes += boxLine(sudoku.getColumn(i));
        }
        strategyCounts[strategy++] += changes;
        return changes;
    }
    
    private int pointingNumbers(SubSudoku s) {
        int changes = 0;
        //remove all possibilities of numbers in the rows/columns outside the intersections
        for (int n = 1; n < length; ++n) {
            SubSudoku row = null;
            SubSudoku column = null;
            boolean rowPointer = false;
            boolean columnPointer = false;
            for (int i = 0; i < length; ++i) {
                Cell c = s.getCell(i);
                if (c.containsPossibility(n)) {
                    //check if all possible n's are in a single row
                    if (row == null) {
                        row = c.getRow();
                        rowPointer = true;
                    } else if (row != c.getRow()) {
                        rowPointer = false;
                    }
                    //check if all possible n's are in a single column
                    if (column == null) {
                        column = c.getColumn();
                        columnPointer = true;
                    } else if (column != c.getColumn()) {
                        columnPointer = false;
                    }
                }
            }
            if (rowPointer) {
                //numbers pointing to a row found
                int cellsModified = 0;
                for (int i = 0; i < length; ++i) {
                    Cell c = row.getCell(i);
                    if (c.getBox() != s) {
                        //remove possibilities of cells in the same row but not in the same box
                        if (c.setPossibile(n, false)) {
                            //System.out.println("Reduction: intersection");
                            ++cellsModified;
                            setCellStrategy(c, strategy);
                        }
                    }
                }
                if (cellsModified > 0) {                      
                    ++changes;
                }
            }
            if (columnPointer) {
                //numbers pointing to a column found
                int cellsModified = 0;
                for (int i = 0; i < length; ++i) {
                    Cell c = column.getCell(i);
                    if (c.getBox() != s) {
                        //remove possibilities of cells in the same column but not in the same box
                        if (c.setPossibile(n, false)) {
                            //System.out.println("Reduction: intersection");
                            ++cellsModified;
                            setCellStrategy(c, strategy);
                        }
                    }
                }
                if (cellsModified > 0) {                      
                    ++changes;
                }
            }
        }
        return changes;
    }
    
    private int boxLine(SubSudoku s) {
        int changes = 0;
        //remove all possibilities of numbers in the boxes outside the intersections
        for (int n = 1; n < length; ++n) {
            SubSudoku box = null;
            boolean boxLine = false;
            for (int i = 0; i < length; ++i) {
                Cell c = s.getCell(i);
                if (c.containsPossibility(n)) {
                    //check if all possible n's are in a single box
                    if (box == null) {
                        box = c.getBox();
                        boxLine = true;
                    } else if (box != c.getBox()) {
                        boxLine = false;
                    }
                }
            }
            if (boxLine) {
                //numbers exclusively intersecting a box found
                int cellsModified = 0;
                for (int i = 0; i < length; ++i) {
                    Cell c = box.getCell(i);
                    if (c.getRow() != s && c.getColumn() != s) {
                        //remove possibilities of cells in the same box but not in the same row/column
                        if (c.setPossibile(n, false)) {
                            //System.out.println("Reduction: intersection");
                            ++cellsModified;
                            setCellStrategy(c, strategy);
                        }
                    }
                }
                if (cellsModified > 0) {                      
                    ++changes;
                }
            }
        }
        return changes;
    }
    
    private int findXWing() {
        int changes = 0;
        changes += setXWing("row");
        changes += setXWing("column");
        for (int i = 0; i < length; ++i) {
            
        }
        strategyCounts[strategy++] += changes;
        return changes;
    }
    
    private int setXWing(String type) {
        int changes = 0;
        int[][] pairs = new int[length][length];
        //find all pairs in puzzle
        for (int i = 0; i < length; ++i) {
            //find all rows/columns with pairs
            SubSudoku s = sudoku.getColumn(i);
            if (type.equals("row")) {
                s = sudoku.getRow(i);
            }
            //find the indexes of hidden pairs
            for (int n = 1; n <= length; ++n) {
                int count = 0;
                int ids = 0;
                OUTER:
                for (int j = 0; j < length; ++j) {
                    if (s.getCell(j).containsPossibility(n)) {
                        ++count;
                        //encode pair cell indexes as single int
                        switch (count) {
                            case 1:
                                ids += j*length;
                                break;
                            case 2:
                                ids += j;
                                break;
                            default:
                                break OUTER;
                        }
                    }
                }
                pairs[i][n-1] = (count == 2) ? ids : -1;
            }
        }
        for (int n = 1; n <= length; ++n) {
            for (int i = 0; i < length-1; ++i) {
                if (pairs[i][n-1] < 0) {
                    continue;
                }
                for (int j = i+1; j < length; ++j) {
                    if (pairs[i][n-1] == pairs[j][n-1]) {
                        //X-wing pair found
                        int cellsModified = 0;
                        //find intersecting rows/columns
                        SubSudoku first = sudoku.getRow(pairs[i][n-1] / length);
                        SubSudoku second = sudoku.getRow(pairs[i][n-1] % length);
                        if (type.equals("row")) {
                            first = sudoku.getColumn(pairs[i][n-1] / length);
                            second = sudoku.getColumn(pairs[i][n-1] % length);
                        }
                        //eliminate from intersecting rows/columns
                        for (int k = 0; k < length; ++k) {
                            if (k != i && k != j) {
                                Cell a = first.getCell(k);
                                Cell b = second.getCell(k);
                                //remove possibility of n in the first intersecting row/column
                                if (a.setPossibile(n, false)) {
                                    //System.out.println("Reduction: x-wing");
                                    ++cellsModified;
                                    setCellStrategy(a, strategy);
                                }
                                //remove possibility of n in the second intersecting row/column
                                if (b.setPossibile(n, false)) {
                                    //System.out.println("Reduction: x-wing");
                                    ++cellsModified;
                                    setCellStrategy(b, strategy);
                                }
                            }
                        }
                        //increment change count only if cells were modified
                        if (cellsModified > 0) {                      
                            ++changes;
                        }
                        break;
                    }
                }
            }
        }
        
        return changes;
    }
    
    private boolean bruteForce() {
        Cell c = getMin();
        int iMin = c.getId()/length;
        int jMin = c.getId()%length;
        //try solving for evey possible value of the selected cell
        for (int n = 1; n <= length; n++) {
            if (c.containsPossibility(n)) {
                //push old values onto stack
                sudokuStack.push(sudoku);
                remainingStack.push(remaining);
                strategyCountStack.push(strategyCounts);
                cellStrategyStack.push(cellStrategies);
                //guess cell as n
                sudoku = new Sudoku(sudoku);
                remaining--;
                strategyCounts = Arrays.copyOf(strategyCounts, strategyCounts.length);
                cellStrategies = Arrays.copyOf(cellStrategies, cellStrategies.length);
                sudoku.getCell(iMin, jMin).setValue(n);
                sudoku.getCell(iMin, jMin).removePossibilities();
                strategyCounts[STRATEGY_NUMBER-1]++;
                cellStrategies[c.getId()] = STRATEGY_NUMBER-1;
                if (recursiveSolve()) {
                    //recursive calls end here
                    return true;
                } else {
                    //pop values from stack
                    sudoku = sudokuStack.pop();
                    remaining = remainingStack.pop();
                    strategyCounts = strategyCountStack.pop();
                    cellStrategies = cellStrategyStack.pop();
                }
            }
        }
        return false;
    }
    
    public Cell getMin() {
        //find cell with smallest number of possibilities
        int min = length+1;
        Cell minCell = null;
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                Cell c = sudoku.getCell(i, j);
                if (c.getPossibilityCount() > 0 && c.getPossibilityCount() < min) {
                    min = c.getPossibilityCount();
                    minCell = c;
                }
            }
        }
        return minCell;
    }
    
    public void updateOriginal() {
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                Cell c = sudoku.getCell(i, j);
                Cell o = original.getCell(i, j);
                o.setValue(c.getValue());
                for (int n = 1; n <= length; n++) {
                    o.setPossibile(n, c.containsPossibility(n));
                }
            }
        }
    }
    
    public static boolean checkValidity(Sudoku s) {
        if (s == null)
            return false;
        int length = s.getLength();
        //create box, row and column sets implented via boolean arrays
        boolean[][] boxes = new boolean[length][length];
        boolean[][] rows = new boolean[length][length];
        boolean[][] columns = new boolean[length][length];
        try {
        for (int i = 0; i < length; i++) {
            //check each box, row, and column for duplicate numbers
            for (int j = 0; j < length; j++) {
                if (boxes[i][s.getBox(i).getCell(j).getValue()-1])
                    return false;
                else
                    boxes[i][s.getBox(i).getCell(j).getValue()-1] = true;
                
                if (rows[i][s.getRow(i).getCell(j).getValue()-1])
                    return false;
                else
                    rows[i][s.getRow(i).getCell(j).getValue()-1] = true;
                
                if (columns[i][s.getColumn(i).getCell(j).getValue()-1])
                    return false;
                else
                    columns[i][s.getColumn(i).getCell(j).getValue()-1] = true;
            }
        }
        } catch (IndexOutOfBoundsException e) {
            //if contains number outside of valid range, return false
            return false;
        }
        return true;
    }
    
    public int getNumericalScore() {
        int[] weights = new int[strategyCounts.length];
        for (int i = 0; i < weights.length; ++i) {
            weights[i] = i;
        }
        int sum = 0;
        for (int i = 0; i < weights.length; i++) 
        {
            sum += strategyCounts[i] * weights[i];    
        }
        return sum; //difficulty score
    }
    
    public String getDifficulty() {
        return "Medium"; //difficulty class
    }
    
    private void writeMatrix() {
        for (int i = 0; i < 9; ++i) {
            if (i % 3 == 0)
                System.out.print(" -----------------------"+System.lineSeparator());
            for (int j = 0; j < 9; ++j) {
                if (j % 3 == 0) System.out.print("| ");
                System.out.print(sudoku.getCell(i, j).getValue() == 0 ? " " : 
                        Integer.toString(sudoku.getCell(i, j).getValue()));

                System.out.print(' ');
            }
            System.out.print("|"+System.lineSeparator());
        }
        System.out.print(" -----------------------"+System.lineSeparator());
    }
    
    //for testing which update algorithm is faster
    public boolean testUpdate(int version) {        
        int changed = 1;
        while (changed == 1) {
            changed = checkColumnsAndRows2(version);
        }
        return true;
    }
    
    private int checkColumnsAndRows2(int version) {
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                Cell c = sudoku.getCell(i, j);
                if (c.getValue() != 0)
                    continue;
                setPossibleValues2(c, version);
                if (c.getPossibilityCount() == 1) {
                    //found a number
                    changes = 1;
                    int n = 1;
                    while (!c.containsPossibility(n)) {
                        ++n;
                    }
                    c.setValue(n);
                    c.setPossibile(n, false);
                }
            }
        }
        return changes;
    }
    
    private void setPossibleValues2(Cell c, int version) {
        for (int i = 1; i <= length; ++i) {
            c.setPossibile(i, true);
        }
        
        if (version == 1) {
            search: for (int i = 1; i <= length; ++i) { 
                for (int j = 0; j < length; ++j) {
                    if (c.getBox().getCell(j).getValue() == i) {
                        c.setPossibile(i, false);
                        continue search;
                    }
                    if (c.getColumn().getCell(j).getValue() == i) {
                        c.setPossibile(i, false);
                        continue search;
                    }
                    if (c.getRow().getCell(j).getValue() == i) {
                        c.setPossibile(i, false);
                        continue search;
                    }
                }
            }
        } else if (version == 2) {
            for (int i = 0; i < length; ++i) {
                if (c.getBox().getCell(i).getValue() != 0) {
                    c.setPossibile(c.getBox().getCell(i).getValue(), false);
                }
                if (c.getColumn().getCell(i).getValue() != 0) {
                    c.setPossibile(c.getColumn().getCell(i).getValue(), false);
                }
                if (c.getRow().getCell(i).getValue() != 0) {
                    c.setPossibile(c.getRow().getCell(i).getValue(), false);
                }
            }
        }
    }
    //end test
    
    //testing naked pair implementations
    public boolean solve2() {
        initializeCells();
        int changed = 1;
        while (changed > 0) {
            update();
            changed = findHiddenSingles();
            if (changed > 0)
                continue;
            changed = findNakedPairs2();
        }
        
        return checkValidity(sudoku);
    }
    
    private int findNakedPairs2() {
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                Cell c = sudoku.getCell(i, j);
                if (c.getPossibilityCount() == 2) {
                    //cell contains a pair
                    int n = 1;
                    while (!c.containsPossibility(n)) {
                        ++n;
                    }
                    int a = n++;
                    while (!c.containsPossibility(n)) {
                        ++n;
                    }
                    int b = n;
                    changes += setNakedPairs2(c, c.getBox(), a, b);
                    changes += setNakedPairs2(c, c.getColumn(), a, b);
                    changes += setNakedPairs2(c, c.getRow(), a, b);
                }
            }
        }
        return changes;
    }
    
    private int setNakedPairs2(Cell c, SubSudoku s, int a, int b) {
        int changes = 0;
        for (int i = 0; i < length; ++i) {
            Cell other = s.getCell(i);
            if (other.getPossibilityCount() == 2 && other.getId() != c.getId() && other.containsPossibility(a) && other.containsPossibility(b)) {
                //if a pair is found, remove the pair from the other cells
                for (int j = 0; j < length; j++) {
                    Cell temp = s.getCell(j);
                    if (temp.getValue() == 0 && i != j && temp.getId() != c.getId()) {
                        //increment change count only if possibilty(s) were set
                        //System.out.println("Reduction: naked pair");
                        changes += (temp.setPossibile(a, false) | temp.setPossibile(b, false)) ? 1 : 0;
                    }
                }
                break;
            }
        }
        return changes;
    }
    //end test
}
