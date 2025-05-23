package UI;
import ConditionPage.ColumnBuilder;
import ConditionPage.NestedCondition;
import Memory.Table;

import javax.swing.*;
import javax.swing.Timer;
import javax.swing.table.DefaultTableModel;

import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.*;
import java.util.List;

public class TableFrame extends JFrame {
    //表数据
    private final Table table;
    // 模拟表数据
    private final LinkedHashMap<String, Object> tableProperties = new LinkedHashMap<>();
    private final LinkedHashMap<String, Table.TableColumn> fields;

    // 页面容器
    private final CardLayout cardLayout = new CardLayout();
    private final JPanel container = new JPanel(cardLayout);

    public TableFrame(Table table) {
        //初始化表数据
        this.table = table;
        this.fields = table.getFields();

        //基础配置
        setTitle("表格管理系统");
        setSize(800, 600);
        setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                // 弹出确认对话框
                int option = JOptionPane.showConfirmDialog(
                        null,
                        "确定要关闭窗口吗？",
                        "确认关闭",
                        JOptionPane.YES_NO_OPTION
                );

                if (option == JOptionPane.YES_OPTION) {
                    table.close();
                    // 关闭窗口并退出程序
                    dispose(); // 释放资源
                    System.exit(0); // 终止程序
                }
                // 如果选择 NO，则不做任何操作
            }
        });





        setLocationRelativeTo(null);

        // 初始化表属性
        initTableProperties();

        // 创建各个页面
        createHomePage();
        createAddPage();
        createDeletePage();
        createQueryPage();
        createColumnsPage();

        // 添加容器到窗口
        add(container);

        // 默认显示首页
        cardLayout.show(container, "HOME");

        //显示
        setVisible(true);
    }

    //表属性
    private void initTableProperties() {
        tableProperties.clear();
        tableProperties.put("表名", table.table_name);
        tableProperties.put("主键", table.getPrimaryKey() + " ");
        tableProperties.put("表文件大小", table.getUsed());
        tableProperties.put("记录字节数组长度", table.getRecord_maxLength());
        tableProperties.put("主键字节数组长度", table.getIndex_length());
        tableProperties.put("行数据字节可拓展的字节数", table.RecordLengthAdd);
        tableProperties.put("页中槽分裂的阈值", table.SlotSplit);
        tableProperties.put("叶子页大小", table.PageLeafSpace + "KB");
        tableProperties.put("非叶子页大小", table.PageNoLeafSpace + "KB");
        tableProperties.put("页分裂阈值", table.PageSplit + "%");
    }

    //---------------------//

    //主页面
    private void createHomePage() {
        JPanel homePanel = new JPanel(new BorderLayout(10, 10));
        homePanel.setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));

        // 表属性展示区域
        final JPanel propertyPanel = new JPanel(new GridLayout(0, 4, 10, 10));
        for (Map.Entry<String, Object> entry : tableProperties.entrySet()) {
            propertyPanel.add(new JLabel(entry.getKey() + ":"));
            propertyPanel.add(new JLabel(entry.getValue().toString()));
        }
        /*
        propertyPanel.add(new JLabel("表空闲空间 :"));
        addButton(propertyPanel,"空闲数组",() ->{JOptionPane.showMessageDialog(propertyPanel,table.getSpare());});
        */

        // 功能按钮区域
        JPanel buttonPanel = new JPanel(new GridLayout(0, 2, 1, 1));
        addButton(buttonPanel, "添加记录", "ADD");
        addButton(buttonPanel, "删除记录", "DELETE");
        addButton(buttonPanel, "查询记录", "QUERY");
        addButton(buttonPanel, "查看表字段", "COLUMNS");
        addButton(buttonPanel,"ROOT(逻辑测试)",() ->{RootFrame.getInstance(this).setVisible(true);
            RootFrame.getInstance(this).toFront();});
        addButton(buttonPanel,"刷新",()->{
                initTableProperties();
                propertyPanel.removeAll();
                for (Map.Entry<String, Object> entry : tableProperties.entrySet()) {
                    propertyPanel.add(new JLabel(entry.getKey() + ":"));
                    propertyPanel.add(new JLabel(entry.getValue().toString()));
                }
                propertyPanel.revalidate();
                propertyPanel.repaint();
            });

        // 布局组装
        homePanel.add(new JLabel("表属性信息", JLabel.CENTER), BorderLayout.NORTH);
        homePanel.add(new JScrollPane(propertyPanel), BorderLayout.CENTER);
        homePanel.add(buttonPanel, BorderLayout.SOUTH);

        container.add(homePanel, "HOME");
    }

    //---------------------//

    //添加页
    private void createAddPage() {
        JPanel addPanel = new JPanel(new BorderLayout(10, 10));

        List<JTextField> texts = new ArrayList<>();
        // 表单区域
        JPanel formPanel = new JPanel(new GridLayout(0, 2, 10, 10));
        for (String column : table.getFieldNamesArr()) {
            formPanel.add(new JLabel(column + ":"));
            JTextField text = new JTextField();
            formPanel.add(text);
            texts.add(text);
        }

        // 创建状态反馈标签
        JLabel statusLabel = new JLabel("", JLabel.RIGHT);
        statusLabel.setPreferredSize(new Dimension(300, 20));
        statusLabel.setFont(new Font("微软雅黑", Font.PLAIN, 12));

        // 按钮区域
        JPanel buttonPanel = new JPanel(new BorderLayout());
        JPanel buttonGroup = new JPanel();

        addButton(buttonGroup, "提交", () -> {
            // 收集数据
            List<String> sts = new ArrayList<>();
            for (JTextField text : texts) {
                sts.add(text.getText());
            }

            // 检查并插入行记录
            try {
                table.insert(sts);
                // 显示成功状态
                showStatus(statusLabel, "添加成功", Color.GREEN.darker());
            } catch (RuntimeException e) {
                // 显示失败状态
                showStatus(statusLabel, "添加失败:" + e.getMessage(), Color.RED);
            }
            clearTextFields(texts);
        });
        addButton(buttonGroup, "返回首页", "HOME");

        // 将按钮组放在左侧，状态标签放在右侧
        buttonPanel.add(buttonGroup, BorderLayout.EAST);
        buttonPanel.add(statusLabel, BorderLayout.WEST);

        addPanel.add(new JLabel("添加新记录", JLabel.CENTER), BorderLayout.NORTH);
        addPanel.add(new JScrollPane(formPanel), BorderLayout.CENTER);
        addPanel.add(buttonPanel, BorderLayout.SOUTH);

        container.add(addPanel, "ADD");
    }

    //上一个方法的辅助方法： 清空输入窗口
    private void clearTextFields(List<JTextField> textFields) {
        for (JTextField textField : textFields) {
            textField.setText(""); // 清空文本内容
            textField.requestFocus(); // 可选：将焦点设置到第一个字段
        }

        // 如果需要将焦点返回到第一个字段
        if (!textFields.isEmpty()) {
            textFields.get(0).requestFocus();
        }
    }

    // 显示状态反馈的方法
    private void showStatus(JLabel label, String message, Color color) {
        label.setText(message);
        label.setForeground(color);

        // 3秒后自动清除状态
        Timer timer = new Timer(3000, e -> label.setText(""));
        timer.setRepeats(false);
        timer.start();
    }

    //---------------------//

    //删除页
    private void createDeletePage() {
        JPanel deletePanel = new JPanel(new BorderLayout(10, 10));


        // 查询条件区域
        JTextField jTextField = new JTextField(20);   //文本框
        JButton delete = new JButton("删除");             //删除按钮
        delete.addActionListener(e -> new SwingWorker<Void,Void>() {
            @Override
            protected Void doInBackground() throws RuntimeException {
                NestedCondition condition = new NestedCondition(jTextField.getText(), table);
                condition.runDelete();
                return null;
            }
            @Override
            protected void done() {
                try {
                    get();
                    JOptionPane.showMessageDialog(deletePanel, "删除成功");
                }catch (Exception ex){
                    JOptionPane.showMessageDialog(deletePanel, ex.getMessage());
                    ex.printStackTrace();
                }
            }
        }.execute());
        JPanel conditionPanel = new JPanel(new FlowLayout());
        conditionPanel.add(new JLabel("删除条件:"));
        conditionPanel.add(jTextField);
        conditionPanel.add(delete);

        // 按钮区域
        JPanel buttonPanel = new JPanel(new GridLayout(2,3,10,10));
        buttonPanel.add(new JLabel());
        JButton merge = new JButton("页合并");             //合并按钮
        merge.addActionListener(e -> new SwingWorker<Void,Void>() {
            @Override
            protected Void doInBackground() throws RuntimeException {
                table.allLeafPageMergeCheck();
                return null;
            }
            @Override
            protected void done() {
                try {
                    get();
                    JOptionPane.showMessageDialog(deletePanel, "已完成所有叶子页的合并检查");
                } catch (Exception ex) {
                    JOptionPane.showMessageDialog(deletePanel, ex.getMessage());
                    ex.printStackTrace();
                }
            }
        }.execute());
        buttonPanel.add(merge);
        buttonPanel.add(new JLabel());
        buttonPanel.add(new JLabel());
        addButton(buttonPanel,"返回首页","HOME");


        deletePanel.add(new JLabel("删除记录", JLabel.CENTER), BorderLayout.NORTH);
        deletePanel.add(conditionPanel,BorderLayout.CENTER);
        deletePanel.add(buttonPanel, BorderLayout.SOUTH);

        container.add(deletePanel, "DELETE");
    }

    //查询页
    private void createQueryPage() {
        JPanel queryPanel = new JPanel(new BorderLayout(10, 10));


        //表格
        JTable jTable = new JTable(new Object[0][],this.table.getFieldNamesArr());
        // 查询条件区域
        JTextField jTextField = new JTextField(20);   //文本框
        JButton search = new JButton("搜索");             //查询按钮
        search.addActionListener(e -> new SwingWorker<DefaultTableModel,Void>() {
            @Override
            protected DefaultTableModel doInBackground() throws RuntimeException {
                String text = jTextField.getText();
                NestedCondition condition = new NestedCondition(text, table);
                Object[][] o = condition.runSearch();
                return new DefaultTableModel(o, table.getFieldNamesArr());
            }
            @Override
            protected void done() {
                try {
                    DefaultTableModel model = get();
                    jTable.setModel(model);
                } catch (Exception ex) {
                    JOptionPane.showMessageDialog(queryPanel, ex.getMessage());
                    ex.printStackTrace();
                }
            }
        }.execute());

        JPanel conditionPanel = new JPanel(new FlowLayout());
        conditionPanel.add(new JLabel("查询条件:"));
        conditionPanel.add(jTextField);
        conditionPanel.add(search);


        // 按钮区域
        JPanel buttonPanel = new JPanel();
        addButton(buttonPanel, "返回首页", "HOME");

        queryPanel.add(conditionPanel, BorderLayout.NORTH);
        queryPanel.add(new JScrollPane(jTable), BorderLayout.CENTER);
        queryPanel.add(buttonPanel, BorderLayout.SOUTH);

        container.add(queryPanel, "QUERY");
    }

    //---------------------//

    //字段页
    private void createColumnsPage() {
        JPanel columnsPanel = new JPanel(new BorderLayout(10, 10));

        //一//

        // 字段列表区域
        JList<String> columnList = new JList<>();
        DefaultListModel<String> listModel = new DefaultListModel<>();     //列表模型
        for (String fieldName : table.getFieldNamesArr())
            listModel.addElement(fieldName);
        columnList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        columnList.setModel(listModel);

        // 字段详情区域
        JTextArea detailArea = new JTextArea(5, 20);
        detailArea.setEditable(false);
        detailArea.setText("选择字段查看详细信息");

        columnList.addListSelectionListener(e -> {
            //getValueIsAdjusting()  这个是操作(这边是鼠标移动)的布尔值 当鼠标不移动后触发
            if (!e.getValueIsAdjusting()) {
                String selected = columnList.getSelectedValue();
                Table.TableColumn column= fields.get(selected);
                detailArea.setText("字段名: " + selected +
                        "\n类型:  " + column.type +
                        "\n长度:  " + column.length +
                        "\n能否为null:  " + column.couldNull +
                        "\n能否重复:  " + column.couldRepeated +
                        "\n记录字节中的位置: " + column.offset +
                        "\n记录数组中的位置: " + column.index);
            }
        });

        // 布局组装
        JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,
                new JScrollPane(columnList), new JScrollPane(detailArea));
        splitPane.setDividerLocation(150);

        //二//
        //插入表字段
        JPanel insert = new JPanel();
        insert.setLayout(new FlowLayout());
        JTextField jTextField = new JTextField(20);   //文本框
        insert.add(new JLabel("插入语句:"));
        insert.add(jTextField);
        addButton(insert,"插入字段",()-> new SwingWorker<Void,Void>(){
            @Override
            protected Void doInBackground() {
                ColumnBuilder.ColumnInserter(jTextField.getText(),table);
                return null;
            }
            @Override
            protected void done(){
                try {
                    get();
                    DefaultListModel<String> newListModel = new DefaultListModel<>();
                    for (String fieldName : table.getFieldNamesArr())
                        newListModel.addElement(fieldName);
                    columnList.setModel(newListModel);
                    createQueryPage();
                    createAddPage();  //重新渲染
                    JOptionPane.showMessageDialog(columnsPanel, "字段添加成功");
                }catch (Exception ex){
                    JOptionPane.showMessageDialog(columnsPanel, ex.getMessage());
                    ex.printStackTrace();
                }
            }
        }.execute());
        //三//

        // 按钮区域
        JPanel buttonPanel = new JPanel();
        addButton(buttonPanel, "返回首页", "HOME");

        columnsPanel.add(insert,BorderLayout.NORTH);
        columnsPanel.add(splitPane, BorderLayout.CENTER);
        columnsPanel.add(buttonPanel, BorderLayout.SOUTH);

        container.add(columnsPanel, "COLUMNS");
    }

    //---------------------//


    // 添加跳转按钮的辅助方法
    private void addButton(JPanel panel, String text, String targetPage) {
        JButton button = new JButton(text);
        button.addActionListener(e -> cardLayout.show(container, targetPage));
        panel.add(button);
    }

    // 添加功能按钮的辅助方法
    private void addButton(JPanel panel, String text, Runnable action) {
        JButton button = new JButton(text);
        button.addActionListener(e -> action.run());
        panel.add(button);
    }

    //返回表数据
    public Table getTable(){return this.table;}

}