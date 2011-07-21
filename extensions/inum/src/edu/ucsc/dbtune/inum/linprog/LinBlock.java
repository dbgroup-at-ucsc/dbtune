package edu.ucsc.dbtune.inum.linprog;
import java.util.ArrayList;

public class LinBlock {


    public float[][] block;
    public int rows;
    public int columns;

    public static int currentConfig;
    public static int currentQ;

    public LinBlock(int rows, int columns) {
        block = new float[rows][columns];
        this.rows = rows;
        this.columns = columns;

        currentConfig = -1;
        currentQ = 0;
    }


    public void copy(int row, int column, LinBlock source) {

        for (int i = row; i < row + source.rows; i++)
            for (int j = column; j < column + source.columns; j++)
                block[i][j] = source.block[i - row][j - column];


    }


    public static LinBlock zero(int rows, int columns) {

        LinBlock result = new LinBlock(rows, columns);
        for (int i = 0; i < rows; i++)
            for (int j = 0; j < columns; j++)
                result.block[i][j] = 0;
        return result;
    }


    public void print(String prefix) {

        for (int i = 0; i < rows; i++) {
            System.out.print(prefix);
            for (int j = 0; j < columns; j++) {
                System.out.print(block[i][j] + " ");
            }
            System.out.println("");
        }
    }


    public void printObjectiveCPLEX(int configSize) {
        System.out.println("CPLEX maximize");
        System.out.print("CPLEX obj: ");

        //Print configuration vars ('x').
        for (int i = 0; i < configSize; i++)
            System.out.print(block[0][i] + " x" + i + " + ");

        //Print candidate vars('y')
        for (int i = configSize; i < columns - 1; i++)
            System.out.print(block[0][i] + " y" + (i - configSize) + " + ");
        System.out.println(block[0][columns - 1] + " y" + (columns - configSize));
    }


    //A bit clumsy but we need the right side as well 
    public void printConstraintsCPLEX(int configSize, LinBlock rightSide) {
        System.out.println("CPLEX Subject To");
        for (int i = 0; i < rows; i++) {
            System.out.print("CPLEX c" + i + ": ");
            for (int j = 0; j < configSize; j++)
                System.out.print(block[i][j] + " x" + j + " + ");
            for (int j = configSize; j < columns - 1; j++)
                System.out.print(block[i][j] + " y" + (j - configSize) + " + ");
            System.out.print(block[i][columns - 1] + " y" + (columns - configSize));
            System.out.println(" <= " + rightSide.block[i][0]);
        }

        System.out.println("CPLEX Bounds");
        for (int j = 0; j < configSize; j++)
            System.out.println("CPLEX 0 <= x" + j + " <= 1");
        for (int j = configSize; j < columns; j++)
            System.out.println("CPLEX 0 <= y" + (j - configSize) + " <= 1");
    }


    //even more clumsy because we need also to print the actual indexes and configurations 
    public void printConstraintsCPLEXComments(int configSize, LinBlock rightSide, ArrayList actualConfigs, LinCand[] actualCandidates) {
        System.out.println("CPLEX Subject To");
        for (int i = 0; i < rows; i++) {
            System.out.print("CPLEX c" + i + ": ");
            for (int j = 0; j < configSize; j++)
                System.out.print(block[i][j] + " x" + j + " + ");
            for (int j = configSize; j < columns - 1; j++)
                System.out.print(block[i][j] + " y" + (j - configSize) + " + ");
            System.out.print(block[i][columns - 1] + " y" + (columns - configSize));
            System.out.println(" <= " + rightSide.block[i][0]);
        }

        System.out.println("CPLEX Bounds");
        for (int j = 0; j < configSize; j++) {
            System.out.print("CPLEX 0 <= x" + j + " <= 1");
            System.out.println(" \\MODEL::" + actualConfigs.get(j));
        }
        for (int j = configSize; j < columns; j++) {
            System.out.print("CPLEX 0 <= y" + (j - configSize) + " <= 1");
            System.out.println(" \\MODEL::" + actualCandidates[j - configSize]);
        }
    }

    public void printConstraintsCPLEXCommentsRow(int configSize, LinBlock rightSide, ArrayList actualConfigs, LinCand[] actualCandidates, int theRow) {
        //System.out.println("CPLEX Subject To");

        int i = 0;
        System.out.print("CPLEX c" + theRow + ": ");
        for (int j = 0; j < configSize; j++) {
            if( j != 0 ) {
                if( block[i][j] > 0.0 ) System.out.print(" + ");
            }
            if( j== 0 || block[i][j] != 0.0 ) System.out.print(block[i][j] + " x" + j);
        }

        for (int j = configSize; j < columns; j++) {
            if( block[i][j] > 0.0 ) System.out.print(" + ");
            if( block[i][j] != 0.0 ) System.out.print(block[i][j] + " y" + (j - configSize));
        }
        System.out.println(" <= " + rightSide.block[i][0]);
    }


    public String toString() {

        String result = new String();
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                result += new String(block[i][j] + " ");
            }
            result += new String("\n");
        }
        return result;
    }
}
