package com.david.completesudoku;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Deque;

/**
 *
 * @author David
 */
public class SudokuGame {
    //constants
    public static final String NEW = "New";
    public static final String IN_PROGRESS = "In progress";
    public static final String COMPLETED = "Completed";

    private String name;
    private String difficulty;
    private String status;
    
    private int score;
    private boolean[] answers;
    private boolean[][] errors;
    private boolean[] hints;
    
    private long currentTime;
    private long elapsed;
        
    private Sudoku sudoku;
    private boolean[][] highlighted;
    private Deque<ActionPair> undo;
    private Deque<ActionPair> redo;
    
    //fields not saved
    private int length;
    private boolean paused;
    private int[][] solved;
    private boolean[][] isErrorShown;

    /**
     * Creates a Sudoku game from given information
     * @param sudoku the sudoku puzzle
     * @param highlighted the highlighted array
     * @param currentTime the last time added to elapsed
     * @param elapsed the time elapsed while solving the puzzle
     * @param name the name of the puzzle
     * @param difficulty the difficulty of the puzzle
     * @param status the current status of the puzzle
     * @param score the score the user has on the puzzle
     * @param answers the answers that were shown to the user
     * @param errors the errors that were shown to the user, last element is whether it was used
     */
    public SudokuGame(Sudoku sudoku, boolean[][] highlighted, long currentTime, long elapsed, 
            String name, String difficulty, String status, int score, boolean[] answers, boolean[][] errors, boolean[] hints) {
        this.sudoku = sudoku;        
        this.highlighted = highlighted;
        this.currentTime = currentTime;
        this.elapsed = elapsed;
        this.name = name;
        this.difficulty = difficulty;
        this.status = status;
        this.score = score;
        this.answers = answers;
        this.errors = errors;
        this.hints = hints;
        
        this.length = sudoku.getLength();
        this.paused = true;
        this.solved = null;
        this.isErrorShown = new boolean[length][length];
    }

    /**
     * Creates a new Sudoku game
     * @param sudoku the sudoku puzzle
     */
    public SudokuGame(Sudoku sudoku) {
        this.sudoku = sudoku;
        this.length = sudoku.getLength();
        this.highlighted = new boolean[length][length];
        this.undo = new ArrayDeque<>();
        this.redo = new ArrayDeque<>();
        this.elapsed = 0;
        this.name = "New Sudoku";
        this.difficulty = "None";
        this.status = NEW;
        this.score = 0;
        this.answers = new boolean[length*length];
        this.errors = new boolean[length*length][length];
        this.hints = new boolean[3];
        this.paused = true;
        this.solved = null;
        this.isErrorShown = new boolean[length][length];
    }
    
    public void reset() {
        int[][] newPuzzle = new int[length][length];
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                if (sudoku.getCell(i, j).isGiven()) {
                    newPuzzle[i][j] = sudoku.getCell(i, j).getValue();
                }
            }
        }
        sudoku = new Sudoku(newPuzzle);
        highlighted = new boolean[length][length];
        undo.clear();
        redo.clear();
        elapsed = 0;
        status = NEW;
        score = 0;
        answers = new boolean[length*length];
        errors = new boolean[length*length][length];
        hints = new boolean[3];
        paused = true;
        isErrorShown = new boolean[length][length];
        begin();
    }
    
    public Sudoku getSudoku() {
        return sudoku;
    }

    public int getLength() {
        return length;
    }

    public boolean[][] getHighlighted() {
        return highlighted;
    }

    public Deque<ActionPair> getRedo() {
        return redo;
    }

    public Deque<ActionPair> getUndo() {
        return undo;
    }

    public void setUndo(Deque<ActionPair> undo) {
        this.undo = undo;
    }

    public void setRedo(Deque<ActionPair> redo) {
        this.redo = redo;
    }

    public class ActionPair {
        Action action;
        Action reverse;

        public ActionPair(Action action, Action reverse) {
            this.action = action;
            this.reverse = reverse;
        }

        public Action getAction() {
            return action;
        }

        public Action getReverse() {
            return reverse;
        }
        
    }
    
    public abstract class Action {          
        public abstract void apply();  
    }
    
    public class SetCellAction extends Action {
        private int targetI;
        private int targetJ;
        private int value;

        public SetCellAction(int targetI, int targetJ, int value) {
            this.targetI = targetI;
            this.targetJ = targetJ;
            this.value = value;
        }
        
        @Override
        public void apply() {
            isErrorShown[targetI][targetJ] = false;
            sudoku.getCell(targetI, targetJ).removePossibilities();
            sudoku.getCell(targetI, targetJ).setValue(value);
        }

        public int getTargetI() {
            return targetI;
        }

        public int getTargetJ() {
            return targetJ;
        }

        public int getValue() {
            return value;
        }
          
    }
    
    public class SetPossibilityAction extends Action {
        private int targetI;
        private int targetJ;
        private int value;
        private boolean possible;

        public SetPossibilityAction(int targetI, int targetJ, int value, boolean possible) {
            this.targetI = targetI;
            this.targetJ = targetJ;
            this.value = value;
            this.possible = possible;
        }
        
        @Override
        public void apply() { 
            isErrorShown[targetI][targetJ] = false;
            sudoku.getCell(targetI, targetJ).setPossibile(value, possible);
            sudoku.getCell(targetI, targetJ).setValue(0);
        }
        
        public int getTargetI() {
            return targetI;
        }

        public int getTargetJ() {
            return targetJ;
        }

        public int getValue() {
            return value;
        }

        public boolean isPossible() {
            return possible;
        }
        
    }
    
    public class FillCellAction extends Action {
        private int targetI;
        private int targetJ;
        private int value;
        private boolean[] possibles;

        public FillCellAction(int targetI, int targetJ, int value, boolean[] possibles) {
            this.targetI = targetI;
            this.targetJ = targetJ;
            this.value = value;
            this.possibles = possibles;
        }
        
        @Override
        public void apply() {
            isErrorShown[targetI][targetJ] = false;
            Cell c = sudoku.getCell(targetI, targetJ);
            for (int n = 1; n <= length; ++n) {
                c.setPossibile(n, possibles[n-1]);
            }
            c.setValue(value);
        }
        
        public int getTargetI() {
            return targetI;
        }

        public int getTargetJ() {
            return targetJ;
        }

        public int getValue() {
            return value;
        }

        public boolean[] getPossibles() {
            return possibles;
        }
        
    }
    
    public class SetHighlightedAction extends Action {  
        private List<Integer> targets;
        private boolean isHighlighted;

        public SetHighlightedAction(int target, boolean isHighlighted) {
            this(new ArrayList<Integer>(), isHighlighted);
            this.targets.add(target);
        }
        
        public SetHighlightedAction(List<Integer> targets, boolean isHighlighted) {
            this.targets = targets;
            this.isHighlighted = isHighlighted;
        }
        
        @Override
        public void apply() {
            for (int t : targets) {
                setHighlighted(t/sudoku.getLength(), t%sudoku.getLength(), isHighlighted);
            }
        }

        public List<Integer> getTargets() {
            return targets;
        }

        public boolean isIsHighlighted() {
            return isHighlighted;
        }
        
    }
    
    public class ShowPossibilitiesAction extends Action {
        
        @Override
        public void apply() {
            Sudoku theoretical = new Sudoku(sudoku);
            for (int i = 0; i < length; ++i) {
                for (int j = 0; j < length; ++j) {
                    if (sudoku.getCell(i, j).getValue() > 0) {
                        theoretical.getCell(i, j).setGiven(true);
                    }
                }
            }
            SudokuSolver s = new SudokuSolver(theoretical);
            s.initializeCells();
            for (int i = 0; i < length; ++i) {
                for (int j = 0; j < length; ++j) {
                    s.setPossibleValues(theoretical.getCell(i, j));
                    for (int n = 1; n <= length; ++n) {
                        sudoku.getCell(i, j).setPossibile(n, theoretical.getCell(i, j).containsPossibility(n));
                    }
                }
            }
        }
        
    }
    
    public class RemovePossibilitiesAction extends Action {
        
        @Override
        public void apply() {
            SudokuSolver s = new SudokuSolver(sudoku);
            for (int i = 0; i < length; ++i) {
                for (int j = 0; j < length; ++j) {
                    sudoku.getCell(i, j).removePossibilities();
                }
            }
        }
        
    }
    
    public class FillPossibilitiesAction extends Action {
        private boolean[][] possibles;

        public FillPossibilitiesAction(boolean[][] possibles) {
            this.possibles = possibles;
        }

        public boolean[][] getPossibles() {
            return possibles;
        }
        
        @Override
        public void apply() {
            SudokuSolver s = new SudokuSolver(sudoku);
            for (int i = 0; i < length*length; ++i) {
                for (int n = 1; n <= length; ++n) {
                    sudoku.getCell(i/length, i%length).setPossibile(n, possibles[i][n-1]);
                }
            }
        }
        
    }
    
    private void resolve(Action action, Action reverse) {
        action.apply();
        undo.push(new ActionPair(action, reverse));
        redo.clear();
    }
    
    public void setEraseAction(int targetI, int targetJ) {
        if (targetI < 0 || targetJ < 0 || targetI >= length || targetJ >= length) {
            throw new IllegalArgumentException();
        }
        Cell c = sudoku.getCell(targetI, targetJ);
        if (c.isGiven() || (c.getValue() == 0 && c.getPossibilityCount() == 0) || status.equals(COMPLETED)) {
            return;
        }
        Action action, reverse;      
        if (c.getValue() > 0) {
            action = new SetCellAction(targetI, targetJ, 0);
            reverse = new SetCellAction(targetI, targetJ, c.getValue());
        } else {
            action = new SetCellAction(targetI, targetJ, 0);
            reverse = new FillCellAction(targetI, targetJ, 0, c.getPossibilities());
        }
        resolve(action, reverse);
    }
    
    public void setValueAction(int targetI, int targetJ, int value) {
        if (value == 0) {
            setEraseAction(targetI, targetJ);
            return;
        }
        if (targetI < 0 || targetJ < 0 || value < 1 || 
                targetI >= length || targetJ >= length || value > length) {
            throw new IllegalArgumentException();
        }
        Cell c = sudoku.getCell(targetI, targetJ);
        if (c.isGiven() || status.equals(COMPLETED)) {
            return;
        }
        Action action, reverse;      
        if (c.getValue() == value) {
            action = new SetCellAction(targetI, targetJ, 0);
            reverse = new SetCellAction(targetI, targetJ, value);
        } else if (c.getPossibilityCount() == 0) {
            action = new SetCellAction(targetI, targetJ, value);
            reverse = new SetCellAction(targetI, targetJ, c.getValue()); 
        } else {
            action = new SetCellAction(targetI, targetJ, value);
            reverse = new FillCellAction(targetI, targetJ, c.getValue(), c.getPossibilities());
        }
        resolve(action, reverse);
        
        //check if solved
        int count = 0;
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                if (sudoku.getCell(i, j).getValue() != 0) {
                    ++count;
                }
            }
        }
        if (count == length*length) {
            boolean valid = true;
            for (int i = 0; i < length; ++i) {
                for (int j = 0; j < length; ++j) {
                    if (sudoku.getCell(i, j).getValue() != getSolved(i, j)) {
                        valid = false;
                    }
                }     
            }
            if (valid) {
                end();
            }
        }
    }
    
    public void setPossibleAction(int targetI, int targetJ, int value) {
        if (value == 0) {
            setEraseAction(targetI, targetJ);
            return;
        }
        if (targetI < 0 || targetJ < 0 || value < 1 || 
                targetI >= length || targetJ >= length || value > length) {
            throw new IllegalArgumentException();
        }
        Cell c = sudoku.getCell(targetI, targetJ);
        if (c.isGiven() || status.equals(COMPLETED)) {
            return;
        }
        Action action, reverse;      
        if (c.getValue() != 0) {
            action = new SetPossibilityAction(targetI, targetJ, value, true);
            reverse = new SetCellAction(targetI, targetJ, c.getValue());
        } else if (c.containsPossibility(value)) {
            action = new SetPossibilityAction(targetI, targetJ, value, false);
            reverse = new SetPossibilityAction(targetI, targetJ, value, true);
        } else {
            action = new SetPossibilityAction(targetI, targetJ, value, true);
            reverse = new SetPossibilityAction(targetI, targetJ, value, false);
        }
        resolve(action, reverse);
    }
    
    public void setPossibilitiesAction(int targetI, int targetJ, boolean[] possibilities) {
        
        if (targetI < 0 || targetJ < 0 || targetI >= length || targetJ >= length) {
            throw new IllegalArgumentException();
        }
        Cell c = sudoku.getCell(targetI, targetJ);
        if (c.isGiven() || status.equals(COMPLETED)) {
            return;
        }
        Action action, reverse;      
        if (c.getPossibilityCount() == 0) {
            action = new FillCellAction(targetI, targetJ, 0, possibilities);
            reverse = new SetCellAction(targetI, targetJ, c.getValue());
        } else {
            action = new FillCellAction(targetI, targetJ, 0, possibilities);
            reverse = new FillCellAction(targetI, targetJ, 0, c.getPossibilities());
        }
        resolve(action, reverse);
    }
    
    public void setHighlightedAction(int targetI, int targetJ) {
        if (targetI < 0 || targetJ < 0 || targetI >= length || targetJ >= length) {
            throw new IllegalArgumentException();
        }
        Action action = new SetHighlightedAction(targetI*length+targetJ, !isHighlighted(targetI, targetJ));
        Action reverse = new SetHighlightedAction(targetI*length+targetJ, isHighlighted(targetI, targetJ));       
        resolve(action, reverse);
    }
    
    public void setHighlightedValueAction(int value) {
        if (value < 0 || value > length) {
            throw new IllegalArgumentException();
        }
        List<Integer> newTargets = new ArrayList<>();
        List<Integer> oldTargets = new ArrayList<>();
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                if (sudoku.getCell(i, j).getValue() == value) {
                    if (isHighlighted(i, j)) {
                        oldTargets.add(i*length+j);
                    } else {
                        newTargets.add(i*length+j);
                    }
                }
            }
        }
        Action action;
        Action reverse;
        if (newTargets.size() > 0) {
            action = new SetHighlightedAction(newTargets, true);
            reverse = new SetHighlightedAction(newTargets, false);
        } else if (oldTargets.size() > 0) {
            action = new SetHighlightedAction(oldTargets, false);
            reverse = new SetHighlightedAction(oldTargets, true);
        } else {
            return;
        }
        resolve(action, reverse);
    }
    
    public void setHighlightedPossibilityAction(int possibility) {
        if (possibility < 1 || possibility > length) {
            throw new IllegalArgumentException();
        }
        List<Integer> newTargets = new ArrayList<>();
        List<Integer> oldTargets = new ArrayList<>();
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                if (sudoku.getCell(i, j).containsPossibility(possibility)) {
                    if (isHighlighted(i, j)) {
                        oldTargets.add(i*length+j);
                    } else {
                        newTargets.add(i*length+j);
                    }
                }
            }
        }
        Action action;
        Action reverse;
        if (newTargets.size() > 0) {
            action = new SetHighlightedAction(newTargets, true);
            reverse = new SetHighlightedAction(newTargets, false);
        } else if (oldTargets.size() > 0) {
            action = new SetHighlightedAction(oldTargets, false);
            reverse = new SetHighlightedAction(oldTargets, true);
        } else {
            return;
        }
        resolve(action, reverse);
    }
    
    public void undo() {
        if (undo.isEmpty()) {
            return;
        }
        ActionPair actionPair = undo.pop();
        actionPair.reverse.apply();
        redo.push(actionPair);
    }
    
    public void redo() {
        if (redo.isEmpty()) {
            return;
        }
        ActionPair actionPair = redo.pop();
        actionPair.action.apply();
        undo.push(actionPair);
    }
    
    public void setHighlighted(int i, int j, boolean highlighted) {  
        this.highlighted[i][j] = highlighted;
    }
    
    public boolean isHighlighted(int i, int j) {
        if (i < 0 || j < 0 || i >= length || j >= length) {
            throw new IllegalArgumentException();
        }    
        return highlighted[i][j];
    }
    
    public boolean isGiven(int i, int j) {
        if (i < 0 || j < 0 || i >= length || j >= length) {
            throw new IllegalArgumentException();
        }    
        return sudoku.getCell(i, j).isGiven();
    }
    
    public int getValue(int i, int j) {
        if (i < 0 || j < 0 || i >= length || j >= length) {
            throw new IllegalArgumentException();
        }    
        return sudoku.getCell(i, j).getValue();
    }
    
    public boolean containsPossibility(int i, int j, int value) {
        if (i < 0 || j < 0 || i >= length || j >= length || value < 1 || value > length) {
            throw new IllegalArgumentException();
        }    
        return sudoku.getCell(i, j).containsPossibility(value);
    }
    
    public boolean[] getPossibilities(int i, int j) {
        if (i < 0 || j < 0 || i >= length || j >= length) {
            throw new IllegalArgumentException();
        }    
        return sudoku.getCell(i, j).getPossibilities();
    }

    public int getPossibilityCount(int i, int j) {
        if (i < 0 || j < 0 || i >= length || j >= length) {
            throw new IllegalArgumentException();
        }
        return sudoku.getCell(i, j).getPossibilityCount();
    }

    public void begin() {
        if (!status.equals(COMPLETED)) {
            start();
            status = IN_PROGRESS;
        }
    }
    
    public void end() {
        if (!status.equals(COMPLETED)) {
            stop();
            undo.clear();
            redo.clear();
            status = COMPLETED;
            score = calculateScore();
        }
    }
    
    public void start() {
        if (paused && !status.equals(COMPLETED)) {
            paused = false;
            currentTime = System.currentTimeMillis();
        }
    }
    
    public void stop() {
        if (!paused && !status.equals(COMPLETED)) {
            paused = true;
            long now = System.currentTimeMillis();
            elapsed += now-currentTime;
            currentTime = now;
        }
    }

    public long getCurrentTime() {
        return currentTime;
    }
    
    public long getElapsed() {
        if (!paused && !status.equals(COMPLETED)) {
            long now = System.currentTimeMillis();
            elapsed += now-currentTime;
            currentTime = now;
        }
        return elapsed;
    }
    
    public String getElapsedFormatted() {
        long seconds = getElapsed() / 1000;
        long s = seconds % 60;
        long m = (seconds / 60) % 60;
        long h = seconds / (60 * 60);
        return String.format("%d:%02d:%02d", h,m,s);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDifficulty() {
        return difficulty;
    }

    public void setDifficulty(String difficulty) {
        this.difficulty = difficulty;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
    
    public int getScore() {
        return score;
    }
    
    public int calculateScore() {
        if (!status.equals(COMPLETED)) {
            return 0;
        }
        Sudoku theoretical = new Sudoku(sudoku);
        boolean answered = false;
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                if (answers[i*length+j]) {
                    theoretical.getCell(i, j).setGiven(true);
                    answered = true;
                }
            }
        }
        SudokuSolver s = new SudokuSolver(theoretical);
        s.initializeCells();
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                for (int n = 1; n <= length; ++n) {
                    if (errors[i*length+j][n-1]) {
                        theoretical.getCell(i, j).setPossibile(n, false);
                    }
                }
            }
        }
        s.solve();
        double adjustedScore = s.getNumericalScore();
        if (answered) {
            adjustedScore *= .6;
        }
        if (hints[0]) {
            adjustedScore *= .7;
        }
        if (hints[1]) {
            adjustedScore *= .8;
        }
        if (hints[2]) {
            adjustedScore *= .9;
        }
        if (elapsed < 1000*60*5) {
            adjustedScore *= 2;
        } else if (elapsed < 1000*60*10) {
            adjustedScore *= 1.5;
        } else if (elapsed < 1000*60*20) {
            adjustedScore *= 1.2;
        } else if (elapsed > 1000*60*60) {
            adjustedScore *= 5;
        }
        
        score = (int) Math.floor(adjustedScore);
        return score;
    }

    public boolean[][] getErrors() {
        return errors;
    }

    public boolean[] getAnswers() {
        return answers;
    }
    
    public boolean[] getHints() {
        return hints;
    }
    
    public void showAllAnswers() {
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                if (sudoku.getCell(i, j).getValue() != getSolved(i, j)) {
                    answers[i*length+j] = true;
                    setValueAction(i, j, getSolved(i, j));
                }
            }
        }
        //highlighted = new boolean[length][length];
        end();
    }
    
    public void showAnswer(int i, int j) {
        answers[i*length+j] = true;
        setValueAction(i, j, getSolved(i, j));
    }
    
    public boolean[][] showAllErrors() {
        boolean[][] mistakes = new boolean[length][length];      
        hints[0] = true;
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                int value = sudoku.getCell(i, j).getValue();
                if (getSolved(i, j) != value && value != 0) {
                    errors[i*length+j][sudoku.getCell(i, j).getValue()-1] = true;
                    mistakes[i][j] = true;
                    isErrorShown[i][j] = true;
                }
            }
        }
        return mistakes;
    }
    
    public boolean showError(int i, int j) {     
        hints[1] = true; 
        int value = sudoku.getCell(i, j).getValue();
        if (getSolved(i, j) != value && value != 0) {
            errors[i*length+j][sudoku.getCell(i, j).getValue()-1] = true;
            isErrorShown[i][j] = true;
            return true;
        }
        return false;
    }
    
    public int getSolved(int targetI, int targetJ) {
        if (solved == null) {
            solved = new int[length][length];
            SudokuSolver s = new SudokuSolver(new Sudoku(sudoku));
            s.solve();
            Sudoku su = s.getSudoku();
            for (int i = 0; i < length; ++i) {
                for (int j = 0; j < length; ++j) {
                    solved[i][j] = su.getCell(i, j).getValue();
                }
            }
        }
        return solved[targetI][targetJ];
    }
    
    public boolean hasUndo() {
        return !undo.isEmpty();
    }
    
    public boolean hasRedo() {
        return !redo.isEmpty();
    }
    
    public void showPossibilities() {
        hints[2] = true;
        boolean[][] old = new boolean[length*length][length];
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                Cell c = sudoku.getCell(i, j);
                if (c.getPossibilityCount() > 0) {
                    old[i*length+j] = c.getPossibilities();
                }
            }
        }
        Action action = new ShowPossibilitiesAction();
        Action reverse = new FillPossibilitiesAction(old);
        
        resolve(action, reverse);
    }
    
    public void removePossibilities() {
        boolean[][] old = new boolean[length*length][length];
        for (int i = 0; i < length; ++i) {
            for (int j = 0; j < length; ++j) {
                Cell c = sudoku.getCell(i, j);
                if (c.getPossibilityCount() > 0) {
                    old[i*length+j] = c.getPossibilities();
                }
                
            }
        }
        Action action = new RemovePossibilitiesAction();
        Action reverse = new FillPossibilitiesAction(old);
        
        resolve(action, reverse);
    }
    
    public boolean isErrorShownInCell(int i, int j) {
        return isErrorShown[i][j];
    }
    
    public void clearShownErrors() {
        isErrorShown = new boolean[length][length];
    }
    
}
