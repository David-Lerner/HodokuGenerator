import com.david.completesudoku.Sudoku;
import com.david.completesudoku.SudokuSolver;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HodokuReader {
    //database path/name
    public static final String DB_PATH = "../data";
    public static final String DB_NAME = "hodoku.db";
    public static final String DB = DB_PATH+"/"+DB_NAME;
    //table naming constants
    public static final String SUDOKU = "sudoku";
    public static final String ID = "id";
    public static final String PUZZLE = "puzzle";
    public static final String DIFFICULTY = "difficulty";
    public static final String GIVEN = "given"; 
    public static final String FAILURE = "failures";
    public static final String URL = "url";
    //reading
    private static final int TOTAL = 2000;
    private static AtomicInteger count;
    private static int total;
    //sudoku size
    public static final int SIZE = 9;
    
    public static void main(String args[]) {
        if(args.length == 0){ 
            System.out.println("No file specified");
            return;
        }
        count = new AtomicInteger();
        try {
            total = Integer.parseInt(args[1]);
            if (total < 1) {
                throw new Exception();
            }
        } catch (Exception e) {
            total = TOTAL;
        }
        File file = new File(args[0]);
        System.out.printf("Reading %d puzzles from %s...%n", total, file.getAbsolutePath());
        try {
            //create path if not exists
            File path = new File(DB_PATH);
            if (!path.exists()) {
                path.mkdir();
            }
            
            Class.forName("org.sqlite.JDBC");
            Connection c = DriverManager.getConnection("jdbc:sqlite:"+DB);
            
            //create table if not exists
            Statement stmt = c.createStatement();
            String sql = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                "%s INTEGER PRIMARY KEY NOT NULL," +
                "%s CHAR(81) NOT NULL," + 
                "%s CHAR(50)," + 
                "%s INTEGER,", SUDOKU, ID, PUZZLE, DIFFICULTY, GIVEN);
            StringBuilder sb = new StringBuilder(sql);
            for (int i = 1; i < SudokuSolver.STRATEGY_NUMBER; i++) {
                sb.append(SudokuSolver.getStrategyName(i).replace(' ', '_'));
                sb.append(" INTEGER,");
            }
            for (int i = 1; i < SudokuSolver.STRATEGY_NUMBER; i++) {
                sb.append(i == 1 ? " Cell_" : ", Cell_");
                sb.append(SudokuSolver.getStrategyName(i).replace(' ', '_'));
                sb.append(" INTEGER");
            }
            sb.append(");");
            stmt.executeUpdate(sb.toString());
  
            if(file.exists() && file.canRead()) {
                Timer timer = new Timer(true);
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        System.out.println((total-count.get())+" puzzles remaining...");
                    }
                }, 0, 4000);
                long fileLength = file.length();
                boolean done = readFile(file, 0L, c);
                while(!done){
                    if(fileLength < file.length()) {
                        done = readFile(file,fileLength, c);
                        fileLength=file.length();
                    }

                }
            }
            c.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName()+": "+e.getMessage());
        }
        System.out.printf("Added %d/%d puzzles%n", count.get(), total);
    }

    public static boolean readFile(File file, Long fileLength, Connection c) throws IOException, SQLException {
        String line = null;

        BufferedReader in = new BufferedReader(new java.io.FileReader(file));
        in.skip(fileLength);
        while((line = in.readLine()) != null)
        {
            Statement stmt = c.createStatement();
            String[] entry = line.split(" ");
            String puzzle = entry[0].replace('.', '0');
            String difficulty = entry[1].substring(1);
            int given = 0;
            char[] chars = puzzle.toCharArray();
            int[][] array = new int[SIZE][SIZE];
            for (int i = 0; i < SIZE; i++) {
                for (int j = 0; j < SIZE; j++) {
                    array[i][j] = chars[i*SIZE+j] - '0'; // char to int
                    if (array[i][j] != 0) {
                        given++;
                    }
                }
            }
            SudokuSolver s = new SudokuSolver(new Sudoku(array));
            s.solve();
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("INSERT INTO %s (%s,%s,%s,", SUDOKU, PUZZLE, DIFFICULTY, GIVEN));
            for (int i = 1; i < SudokuSolver.STRATEGY_NUMBER; i++) {
                sb.append(SudokuSolver.getStrategyName(i).replace(' ', '_'));
                sb.append(',');
            }
            for (int i = 1; i < SudokuSolver.STRATEGY_NUMBER; i++) {
                sb.append(i == 1 ? "Cell_" : ",Cell_");
                sb.append(SudokuSolver.getStrategyName(i).replace(' ', '_'));
            }
            sb.append(String.format(") VALUES ('%s','%s',%d,", puzzle, difficulty, given));
            for (int i = 1; i < SudokuSolver.STRATEGY_NUMBER; i++) {
                sb.append(s.getStrategyCount(i));
                sb.append(',');
            }
            for (int i = 1; i < SudokuSolver.STRATEGY_NUMBER; i++) {
                if (i > 1) {
                    sb.append(',');
                }
                sb.append(s.getStrategyCountByCell(i));
            }
            sb.append(");");
            stmt.executeUpdate(sb.toString());
            stmt.close();
            if (count.incrementAndGet() == total) {
                in.close();
                return true;
            }
        }
        in.close();
        return false;
    }
}
