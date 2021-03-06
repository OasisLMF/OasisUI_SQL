
#' Process Run Page
#' @rdname processRunPage
#' @description Server logic for the process run page
#' @inheritParams flamingoModule
#' @param userId reactive expresssion yielding user id
#' @param preselRunId reactive expresssion yielding preselected run id
#' @param preselProcId reactive expression yielding preselected proc id
#' @param logMessage function that will be passed info messages
#' @param reloadMillis amount of time to wait between table updates;
#' see \link{invalidateLater}; 
#' @return empty list
#' @importFrom DT renderDataTable
#' @importFrom shinyjs enable disable show hide
#' @importFrom plotly renderPlotly
#' @export
processRunPage <- function(
    input,
    output,
    session,
    dbSettings,
    apiSettings,
    userId,
    preselRunId = reactive(-1),
    preselProcId = reactive(-1),
    progOasisId = reactive(-1),
    active = reactive(TRUE),
    logMessage = message,
    reloadMillis = 10000) {
  
  ns <- session$ns
  
  result <- reactiveValues(
      
      processRunId = -1,
      processRunStatus = NULL,
  
      prcrundata = NULL,
      prcrundataCounter = 0,
      
      filesListData = NULL,
      fileData = NULL,
      
      ProcessData = NULL,
  
      outputPlotData = NULL
      
  )
  
  reloadRunData <- function() {
    result$prcrundataCounter <- isolate(result$prcrundataCounter + 1)
  }
  
  
  
  ### Process Data Table
  
  observe(if (active()) {
        
        # reload when page becomes active
        result$ProcessData <- getProcessData(dbSettings, userId(), 0, 0, 0)
        
      })
  
  output$tableprocessdata2 <- renderDataTable({
        
        index <- match(c(preselProcId()),result$ProcessData[[1]])
        
        datatable(
            result$ProcessData,
            class = "flamingo-table display",
            rownames = TRUE,
            filter = "bottom",
            selection = list(mode = 'single',
                selected = rownames(result$ProcessData)[c(as.integer(index))]),
            colnames = c('Row Number' = 1),
            options = getPRTableOptions()
        )
        
      })
  
  
  
  ### Process Run Table
  
  getProcessRunWithUserChoices <- function(pruser, prmodel, prprogramme,
      prworkflow) {
    
    prtable <- getProcessData(dbSettings, pruser, prmodel, prprogramme, prworkflow)
    
    # Rule is, for one process ID, pass that process ID in, for all
    # processes pass a null.  For processes in all states (completed,
    # created, in progress etc), pass 'All', for just in progress pass
    # 'In Progress'
    prcid <- toString(prtable[c(input$tableprocessdata2_rows_selected), 1])
    
    AllOrInProgress <- isolate(input$radioprrunsAllOrInProgress)
    
    if (AllOrInProgress == "In_Progress") {
      AllOrInProgress = "In Progress"
    }
    
    result$prcrundata <- getProcessRun(dbSettings, prcid, AllOrInProgress)
    
  }
  
  observe(if (active()) {
        
        # reload after pressing refresh
        force(input$abuttonrefreshprrun)
        
        # reload automatically every so often
        invalidateLater(reloadMillis)
        
        # reload after reloadRunData is called 
        force(result$prcrundataCounter)
        
        # reload if radio buttons for 'All' vs 'In_Progress' change
        force(input$radioprrunsAllOrInProgress)
        
        if (length(input$tableprocessdata2_rows_selected) > 0) {
          
          show("prruntable")
          
          getProcessRunWithUserChoices(userId(), 0, 0, 0)
          
        } else {
          
          hide("prruntable")
          hide("prrunlogtable")
          
        }
        
        logMessage("process run table refreshed")
        
      })
  
  output$processrundata <- renderDataTable(if (!is.null(result$prcrundata)) {
        
        if (preselRunId() == -1) {
          index <- 1
        } else {
          index <- match(c(preselRunId()), result$prcrundata[[1]])
        }
        
        datatable(
            result$prcrundata,
            class = "flamingo-table display",
            rownames = TRUE,
            selection = list(mode = 'single',
                selected = rownames(result$prcrundata)[c(as.integer(index))]),
            colnames = c('Row Number' = 1),
            options = getPRTableOptions()
        )
        
      })
  
  
  
  ### Output Files Table
  
  observe(if (length(row <- input$processrundata_rows_selected) > 0) {
            
          result$processRunId <- result$prcrundata[row, 1][length(result$prcrundata[row, 1])]
          result$processRunStatus <- result$prcrundata[row, 4]
          
          result$filesListData <- getFileList(dbSettings, result$processRunId)
          
          # manual refresh button
          force(input$abuttonrefreshprrunoutputfile)
          
          # reload automatically every so often
          invalidateLater(reloadMillis)
          
          logMessage("output files table refreshed")
          
        } else {
          
          result$processRunId <- -1
          result$processRunStatus <- NULL
          
        })
  
  observeEvent(input$abuttondisplayoutput, if (result$processRunId != -1) {
          
          show("prrunoutput")
          updateTabsetPanel(session, "tabsetprrunoutput", selected = "tabprrunfilelist")
          
          hide("abuttondisplayoutput")
          show("abuttonhideoutput")
          
        } else {
          
          showNotification(type = "warning", "Please select a Process Run first")
          
        })
  
  observeEvent(input$abuttonhideoutput, {
        
        hide("prrunoutput")
        hide("abuttonhideoutput")
        show("abuttondisplayoutput")
        
      })
  
  output$outputfileslist <- renderDataTable(
      if (!is.null(result$filesListData)) {
          
          datatable(
              result$filesListData,
              class = "flamingo-table display",
              rownames = TRUE,
              selection = "single",
              colnames = c('Row Number' = 1),
              options = getPRTableOptions()
          )
          
      })
  
  

  ### Process Run Output
  
  ## file data
  
  observe(if (active() && input$tabsetprrunoutput == "tabprrunfiledata") {
        
          if (length(row <- input$outputfileslist_rows_selected) > 0) {
            
            filename <- file.path(
                result$filesListData[row, 5],
                result$filesListData[row, 2])
            
            tryCatch({
                  
                  result$fileData <- read.csv(filename, header = TRUE, sep = ",",
                      quote = "\"", dec = ".", fill = TRUE, comment.char = "")
                  
                }, error = function(e) {
                  
                  showNotification(
                      sprintf("Could not fetch file contents: %s", e$message),
                      type = "error")
                  
                })
            
          } else {
            
            showNotification(type = "warning",
                "Please select a file from File List.") 
            updateTabsetPanel(session, "tabsetprrunoutput",
                selected = "tabprrunfilelist")
            
          }
      })
  
  output$dttableoutputfiledata <- renderDataTable({
        datatable(
            result$fileData,
            class = "flamingo-table display",
            rownames = TRUE,
            selection = "none",
            colnames = c('Row Number' = 1)
        )
      })
  
  ## summary plots and table
  
  observe(if (active() && result$processRunId > -1) {
        
        # invalidate on status change
        force(result$processRunStatus)
        
        stmt <- buildDbQuery("getOutputSummaryEP", result$processRunId)
        result$outputPlotData <- executeDbQuery(dbSettings, stmt)
          
      })
      
  output$plotGULOutput <- renderPlotly({
        
        outputPlotData <- result$outputPlotData
        
        if (!is.null(outputPlotData)) {
          plotGUL(outputPlotData, interactive = TRUE)
        }
        
      })
  
  output$plotILOutput <- renderPlotly({
        
        outputPlotData <- result$outputPlotData
        
        if (!is.null(outputPlotData)) {
          plotIL(outputPlotData, interactive = TRUE)
        }
        
      })
  
  output$dttableoutputsummary <- renderDataTable({
        
        # invalidate on status change
        force(result$processRunStatus)
        
        outputSummaryData <- executeDbQuery(dbSettings,
            paste("exec getOutputSummary", result$processRunId))
        
        datatable(
            outputSummaryData,
            class = "flamingo-table display",
            rownames = TRUE,
            selection = "none",
            colnames = c('Row Number' = 1),
            options = getPRTableOptions()
        )
        
      })
  
  ## export
  
  output$PRfiledataIdownloadexcel <- downloadHandler(
      filename = paste0(result$filesListData[c(input$outputfileslist_rows_selected), 2]),
      content = function(file) {
        write.csv(result$fileData, file)}
  )

  
  
  ### Log Table
  
  # when a ProcessRun is selected in processrundata table    
  observe(if (active()) {
        
        if (length(input$processrundata_rows_selected) > 0) {
          
          updateTabsetPanel(session, "tabsetprrunoutput", selected = "tabprrunfilelist")
          show("prrunlogtable")
          
        } else {
          
          show("abuttondisplayoutput")
          hide("abuttonhideoutput")
          hide("prrunoutput")
          hide("prrunlogtable")
          
        }
        
      })
  
  output$log <- renderDataTable({
        
        if (length(row <- input$processrundata_rows_selected) > 0) {
          
          # manual refresh button
          force(input$abuttonrefreshprrunlogs)
          
          # reload automatically every so often
          invalidateLater(reloadMillis)
          
          wfid <- result$prcrundata[row, 1][length(result$prcrundata[row, 1])]
          
          logMessage("log table refreshed")
          
          datatable(
              getProcessRunDetails(dbSettings, wfid),
              class = "flamingo-table display",
              rownames = TRUE,
              selection = "none",
              colnames = c('Row Number' = 1),
              options = getPRTableOptions()
          )
          
        }
      })
  
  
  
  ### Run Process Dialog
  
  # Run Process: when a row in tableprocessdata2 is selected
  # and Run Process button is clicked
  runProcessDialog <- function() {
    
    if (!length(row <- input$tableprocessdata2_rows_selected) > 0 ) {
      
      showNotification(type = "warning", "Please select a Process to run.")
      
    } else {
      
      toggleModal(session, "bsmodalrunparam", toggle = "open")
      clearotherparams()
      clearchkboxgrp()
      
      prtable <- getProcessData(dbSettings, userId(), 0, 0, 0)
      procId <- toString(prtable[row, 1][length(prtable[row, 1])])
      
      paramlist <- executeDbQuery(dbSettings,
          buildDbQuery("getRuntimeParamList", procId))
      
      hide("perilwind") 
      hide("perilsurge")
      hide("perilquake") 
      hide("perilflood")
      hide("demandsurge")
      hide("leakagefactor")
      
      if (nrow(paramlist) > 0) {
        for (i in 1:nrow(paramlist)) {
          ctrname <- gsub("_", "", paramlist[i, 1], fixed = TRUE)
          show(ctrname)
        }
      }
      
    }
  }
  
  observeEvent(input$abuttonrunpr, runProcessDialog())
  
  onclick("abuttonrerunpr", {
        if (length(input$processrundata_rows_selected) > 0) {
          prrunid <- (result$prcrundata[c(input$processrundata_rows_selected), 1][length(result$prcrundata[c(input$processrundata_rows_selected), 1])])
          outputlist <- executeDbQuery(dbSettings, paste0("exec dbo.getOutputOptionOutputs @processrunid = ", prrunid))
          runparamsforpr <- executeDbQuery(dbSettings, paste0("exec dbo.getProcessRunParams ", prrunid))
          
          runProcessDialog()
          
          updateTextInput(session, "tinputprocessrunname", value = result$prcrundata[c(input$processrundata_rows_selected), 2])
          
          if (nrow(runparamsforpr) > 0) {
            for (i in 1:nrow(runparamsforpr)) {
              if (runparamsforpr[i,1] == "number_of_samples") {
                updateTextInput(session, "tinputnoofsample", value = runparamsforpr[i,2])      
                next
              }
              if (runparamsforpr[i,1] == "gul_threshold") {
                updateTextInput(session, "tinputthreshold", value = runparamsforpr[i,2])
                next
              }
              if (runparamsforpr[i,1] == "event_set") {
                updateSelectInput(session, "sinputeventset", selected = runparamsforpr[i,2])
                next
              }
              if (runparamsforpr[i,1] == "event_occurrence_id") {
                updateSelectInput(session, "sinputeventocc", selected = runparamsforpr[i,2])
                next
              }
              if (runparamsforpr[i,1] == "peril_wind") {
                updateCheckboxInput(session, "chkinputprwind", value = eval(parse(text=toString(runparamsforpr[i,2]))))
                next
              }
              if (runparamsforpr[i,1] == "peril_surge") {
                updateCheckboxInput(session, "chkinputprstsurge", value = eval(parse(text=toString(runparamsforpr[i,2]))))
                next
              }
              if (runparamsforpr[i,1] == "peril_quake") {
                updateCheckboxInput(session, "chkinputprquake", value = eval(parse(text=toString(runparamsforpr[i,2]))))
                next
              }
              if (runparamsforpr[i,1] == "peril_flood") {
                updateCheckboxInput(session, "chkinputprflood", value = eval(parse(text=toString(runparamsforpr[i,2]))))
                next
              }
              if (runparamsforpr[i,1] == "demand_surge") {
                updateCheckboxInput(session, "chkinputdsurge", value = eval(parse(text=toString(runparamsforpr[i,2]))))
                next
              }
              if (runparamsforpr[i,1] == "leakage_factor") {
                updateSliderInput(session, "sliderleakagefac", value = runparamsforpr[i,2])
                next
              }
            }
          }
          orows <- nrow(outputlist)
          if (orows > 0) {
            for (i in 1:orows) {
              grpid <- paste0("chk",outputlist$Group[i])
              grpinputid <- strsplit(toString(grpid), " ")[[1]]
              chkboxid <- outputlist$Parameter[i]
              selchoices <- as.list(strsplit(toString(chkboxid), ",")[[1]])
              updateCheckboxGroupInput(session, inputId = grpinputid, selected = c(selchoices))
            }
          }  
        } else {
          showNotification(type = "warning", "Please select Process Run")
        }
        
      })
  
  
  
  ### Run Process
  
  # reactive expression yielding the output options as a list
  outputOptionsList <- reactive(paste(collapse = ",", c(
              input$chkgulprog, input$chkgulpolicy,
              input$chkgulstate, input$chkgulcounty, input$chkgulloc,
              input$chkgullob, input$chkilprog, input$chkilpolicy,
              input$chkilstate, input$chkilcounty, input$chkilloc,
              input$chkillob, input$chkriprog, input$chkripolicy,
              input$chkristate, input$chkricounty, input$chkriloc,
              input$chkrilob)))
  
  # Update button in sidebar panel to update checkboxes for pre-populated values
  observe(if (active()) {
        
        clearchkboxgrp()
        if (length(input$sinoutputoptions) > 0 && input$sinoutputoptions != "<Select>") {
          
          outputlist <- executeDbQuery(dbSettings,
              buildDbQuery("getOutputOptionOutputs", input$sinoutputoptions))
          
          if (nrow(outputlist) > 0) {
            for (i in 1:nrow(outputlist)) {
              grpid <- paste0("chk",outputlist$Group[i])
              grpinputid <- strsplit(toString(grpid), " ")[[1]]
              chkboxid <- outputlist$Parameter[i]
              selchoices <- as.list(strsplit(toString(chkboxid), ",")[[1]])
              updateCheckboxGroupInput(session, inputId = grpinputid, selected = c(selchoices))
            }
          }
        }
      })
  
  
  clearOutputOptions <- function() {
    updateSelectInput(session, "sinoutputoptions",
        choices = c("<Select>", getOutputOptions(dbSettings)),
        selected = "<Select>")
  }
  
  # Clear the checkbox groups and preset dropdown
  onclick("abtnclroutopt", {
        
        clearchkboxgrp()
        clearOutputOptions()
        
      })
  
  # Save output for later use as presets
  onclick("abuttonsaveoutput", {
        
        if (outputOptionsList() != "") {
          
          toggleModal(session, "bsmodalsaveoutput", toggle = "open")
          
        } else {
          
          toggleModal(session, "bsmodalsaveoutput", toggle = "close")
          showNotification(type = "warning", "Please select Output")
          
        }
        
      })  
  
  onclick("abuttonsubmitoutput", {
        
        if (input$tinputoutputname == "") {
          
          showNotification(type = "warning", "Please enter Output Name")
          
        } else {
          
          stmt <- paste0("exec dbo.saveoutputoption @OutputOptionName = '",
              input$tinputoutputname, "',@OutputOptionsList = '",
              outputOptionsList(), "'")
          executeDbQuery(dbSettings, stmt)
          
          updateTextInput(session, "tinputoutputname", value = "")
          clearOutputOptions()
          
          showNotification(type = "message", "Output saved.")
          
          toggleModal(session, "bsmodalsaveoutput", toggle = "close")
        }
        
      })
  
  
  # A function to generate process run
  generateRun <- function() {
    
    prTable <- getProcessData(dbSettings, userId(), 0, 0, 0)
    progOasisID <- toString(prTable[input$tableprocessdata2_rows_selected, 1])
    
    processrunname <- isolate(input$tinputprocessrunname)
    nosample <- isolate(input$tinputnoofsample)
    sthreshold <- isolate(input$tinputthreshold)
    eventsetid <- isolate(input$sinputeventset)
    eventoccid <- isolate(input$sinputeventocc)
    
    windperil <- NULL
    surgeperil <- NULL
    quakeperil <- NULL
    floodperil <- NULL
    dmdsurge <- NULL
    leakagefactor <- NULL
    
    summaryreports <- tolower(isolate(input$chkinputsummaryoption))
    
    # functionality to handle model resource based metrics
    stmt <- buildDbQuery("getRuntimeParamList", progOasisID)
    runparamlist <- executeDbQuery(dbSettings, stmt)
    
    rows <- nrow(runparamlist)
    
    if (rows > 0) {
      for (i in 1:rows) {
        if (runparamlist[i, 1] == 'demand_surge') {
          dmdsurge <- tolower(isolate(input$chkinputdsurge))
          next
        } 
        if (runparamlist[i, 1] == 'peril_wind') {
          windperil <- tolower(isolate(input$chkinputprwind))
          next
        }
        if (runparamlist[i, 1] == 'peril_surge') {
          surgeperil <- tolower(isolate(input$chkinputprstsurge))
          next
        }
        if (runparamlist[i, 1] == 'peril_quake') {
          quakeperil <- tolower(isolate(input$chkinputprquake))
          next
        }
        if (runparamlist[i, 1] == 'peril_flood') {
          floodperil <- tolower(isolate(input$chkinputprflood))
          next
        }
        if (runparamlist[i, 1] == 'leakage_factor') {
          leakagefactor <- isolate(input$sliderleakagefac)
        }
      }
    }
    
    outputsStringGUL <- paste(collapse = ", ",
        c(input$chkgulprog, input$chkgulpolicy, input$chkgulstate,
            input$chkgulcounty, input$chkgulloc, input$chkgullob))
    
    outputsStringIL <- paste(collapse = ", ",
        c(input$chkilprog, input$chkilpolicy, input$chkilstate,
            input$chkilcounty, input$chkilloc, input$chkillob))

    outputsStringRI <- paste(collapse = ", ",
        c(input$chkriprog, input$chkripolicy, input$chkristate,
            input$chkricounty, input$chkriloc, input$chkrilob))
    
    stmt <- paste0("exec dbo.WorkflowFlattener ",
        "@ProgOasisID= ", progOasisID, ", ",
        "@WorkflowID= 1", ", ",
        "@NumberOfSamples=", nosample, ", ",
        "@GULThreshold= ", sthreshold, ", ",
        "@UseRandomNumberFile= 0, ",
        "@OutputsStringGUL= '", outputsStringGUL, "', ",
        "@OutputsStringIL= '", outputsStringIL, "', ",
        "@OutputsStringRI= '", outputsStringRI, "', ",
        "@EventSetID= '", eventsetid ,"', ",
        "@EventOccurrenceID= '", eventoccid, "', ", 
        "@PerilWind = '", windperil ,"', ",
        "@PerilSurge='", surgeperil, "', ",
        "@PerilQuake='", quakeperil, "', ",
        "@PerilFlood='", floodperil, "', ",
        "@DemandSurge= '", dmdsurge, "', ",
        "@LeakageFactor= '" , leakagefactor, "', ",
        "@ProcessRunName= '" , processrunname, "', ",
        "@SummaryReports='", summaryreports , "'")
    
    logMessage(paste("Workflow flattener query: ", stmt))
    runId <- executeDbQuery(dbSettings, stmt)
    logMessage(paste("Process Run ID: ", runId))
    
    return(runId)
  }
  
  # Execute Process run: When "Execute Run" button is clicked
  onclick("abuttonexecuteprrun", {
        
        if (outputOptionsList() == "") {
          
          showNotification(type = "warning", "Please select Output")
          
        } else {
          
          runId <- generateRun()
          toggleModal(session, "bsmodalrunparam", toggle = "close")
          clearchkboxgrp()
          clearotherparams()
          if (is.null(runId)) {
            showNotification(type = "error",
                "Process Run ID could not be generated. So process run cannot be executed.")
          } else {      
            status <- runProcess(apiSettings, runId)
            logMessage(sprintf("runProcess status: %s", status))
            if (grepl("success", status, ignore.case = TRUE)) {
              showNotification(type = "message", 
                  sprintf("Created Process Run ID: %s and process run is executing.",
                      runId))
              reloadRunData()
            } else {
              showNotification(type = "warning",
                  sprintf("Created Process Run ID: %s. But process run executing failed.",
                      runId))
            }
          }
        }
      })
  
  # Cancel Process run: When "Cancel" button is clicked
  onclick("abuttoncancelrun", {
        toggleModal(session, "bsmodalrunparam", toggle = "close")
        clearchkboxgrp()
        clearotherparams()
      })
  
      
      ### When Activated (e.g. tab openened)
      
      observe(if (active()) {
            
            show("perilwind")
            show("perilsurge")
            show("perilquake")
            show("perilflood")
            show("demandsurge")
            show("leakagefactor")
            
            disable("chkgulpolicy")
            
            disable(selector = "#chkgulcounty input[value='gulcountyFullUncAEP']")
            disable(selector = "#chkgulcounty input[value='gulcountyFullUncOEP']")
            disable(selector = "#chkgulcounty input[value='gulcountyAEPWheatsheaf']")
            disable(selector = "#chkgulcounty input[value='gulcountyOEPWheatsheaf']")
            
            disable(selector = "#chkgulloc input[value='gullocFullUncAEP']")
            disable(selector = "#chkgulloc input[value='gullocFullUncOEP']")
            disable(selector = "#chkgulloc input[value='gullocAEPWheatsheaf']")
            disable(selector = "#chkgulloc input[value='gullocOEPWheatsheaf']")
            
            disable(selector = "#chkilcounty input[value='ilcountyFullUncAEP']")
            disable(selector = "#chkilcounty input[value='ilcountyFullUncOEP']")
            disable(selector = "#chkilcounty input[value='ilcountyAEPWheatsheaf']")
            disable(selector = "#chkilcounty input[value='ilcountyOEPWheatsheaf']")
            
            disable(selector = "#chkilloc input[value='illocFullUncAEP']")
            disable(selector = "#chkilloc input[value='illocFullUncOEP']")
            disable(selector = "#chkilloc input[value='illocAEPWheatsheaf']")
            disable(selector = "#chkilloc input[value='illocOEPWheatsheaf']")
            
          })

      
      
      ### Helper functions
      
      # table settings for pr tab: returns option list for datatable
      getPRTableOptions <- function() {
        
        options <- list(
            search = list(caseInsensitive = TRUE), 
            processing = 0,
            scrollX = TRUE,
            pageLength = 10,
            columnDefs = list(list(visible = FALSE, targets = 0)))
        
        return(options)
        
      }
      
      # Clear checkboxgroup
      clearchkboxgrp <- function() {
        
        # GUL group
        updateCheckboxGroupInput(session, inputId ="chkgulprog", selected = "None")
        disable("chkgulpolicy")
        updateCheckboxGroupInput(session, inputId ="chkgulstate", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkgulcounty",selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkgulloc", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkgullob", selected = "None")
        
        # IL group
        updateCheckboxGroupInput(session, inputId ="chkilprog", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkilpolicy", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkilstate", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkilcounty", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkilloc", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkillob", selected = "None")
        # RI group
        updateCheckboxGroupInput(session, inputId ="chkriprog", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkripolicy", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkristate", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkricounty", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkriloc", selected = "None")
        updateCheckboxGroupInput(session, inputId ="chkrilob", selected = "None")
        
      }
      
      # Clear other runtime params
      clearotherparams <- function() {
        
        updateSelectInput(session, "sinoutputoptions",
            choices = c("<Select>", getOutputOptions(dbSettings)),
            selected = "<Select>")
        updateTextInput(session, "tinputprocessrunname", value = "")
        
        progOasisId <- toString(result$ProcessData[c(input$tableprocessdata2_rows_selected), 1])
        
        updateSliderInput(session, "sliderleakagefac", "Leakage factor:",
            min= 0, max=100, value=0.5, step = 0.5)
        updateSelectInput(session, "sinputeventset",
            choices = getEventSet(dbSettings, progOasisId))
        updateSelectInput(session, "sinputeventocc",
            choices = getEventOccurrence(dbSettings, progOasisId))  
        updateCheckboxInput(session, "chkinputprwind", "Peril: Wind",
            value = TRUE)
        updateCheckboxInput(session, "chkinputprstsurge", "Peril: Surge",
            value = TRUE)
        updateCheckboxInput(session, "chkinputprquake", "Peril: Quake",
            value = TRUE)
        updateCheckboxInput(session, "chkinputprflood", "Peril: Flood",
            value = TRUE)
        updateCheckboxInput(session, "chkinputdsurge", "Demand Surge",
            value = TRUE)
      }
      
      
      
      ### Module Output
      
      moduleOutput <- list()
      
      return(moduleOutput)
  
}
