package com.david.completesudoku;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author David
 */
public class SudokuGenerator {

    private static final String DEFAULT_GAME = "puzzler.txt";
    
    public static final String RANDOM = "Random";
    public static final String DIABOLICAL = "Diabolical";
    public static final String EASY = "Easy";
    public static final String HARD = "Hard";
    public static final String MEDIUM = "Medium";
    public static final String ULTRA_EASY = "Ultra Easy";

    
    private int sudokuNumber;
    private List<int[][]> sudokus = new ArrayList<>();
    private List<String> difficulties = new ArrayList<>();
    
    public SudokuGenerator() {
        sudokuNumber = 0;
        sudokus = new ArrayList<>();
        
        //from file
        try (BufferedReader br = new BufferedReader(new FileReader(DEFAULT_GAME))) {
            String line;  
            while((line = br.readLine()) != null){
                if (line.contains("G")) {
                    String[] metaData = line.split(" ");
                    if (metaData.length > 2) {
                        difficulties.add(metaData[2]);
                    }
                    char[][] initialBoard = new char[9][9];
                    for (int i = 0; i < 9; i++) {
                        line = br.readLine();
                        initialBoard[i] = line.toCharArray();
                    }
                    int[][] grid = new int[9][9];
                    for (int i = 0; i < 9; i++) {
                        for (int j = 0; j < 9; j++) {
                            grid[i][j] = Character.getNumericValue(initialBoard[i][j]);
                        }
                    }
                    sudokus.add(grid);
                }
            }
        } catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + DEFAULT_GAME + "'");                
        } catch(IOException ex) {
            System.out.println("Error reading file '" + DEFAULT_GAME + "'");                   
        }
        
    }
    
    public Sudoku getSudoku(String difficulty) {
        if (difficulty.equals(RANDOM)) {
            return new Sudoku(sudokus.get(sudokuNumber++));
        }
        else {
            int i = 0;
            while (!difficulties.get(i).equals(difficulty)) {
                ++i;
            }
            if (i < sudokus.size()) {
                return new Sudoku(sudokus.get(i));
            }
        }
        return null;
    }
    
    public List<int[][]> getTestSudokus() {
        return sudokus;
    }
}
