package com.david.completesudoku;

/**
 *
 * @author David
 */
public interface SudokuModel {
    
    /**
     * Saves a Sudoku game to a database
     * @param sudokuGame the game to save
     * @param param other info necessary for a particular implementation
     */
    void saveGame(SudokuGame sudokuGame, Object param);
    
    /**
     * Loads a Sudoku game from a database
     * @param param info necessary for a particular implementation
     * @return the Sudoku game loaded
     */
    SudokuGame loadGame(Object param);
}
