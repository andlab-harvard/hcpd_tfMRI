## R setup
rm(list = ls())
setwd('/ncf/hcp/data/analyses/myelin/fmri_test/')

library(ciftiTools)
ciftiTools.setOption('wb_path', '/n/helmod/apps/centos7/Core/connectome-workbench/1.3.2-fasrc01/')  

#####################################
## Correct Rejection - GO Contrast ##
#####################################

## Load Connectome workbench
system("module load connectome-workbench/1.3.2-fasrc01")

## Read in CR-GO contrast xifti file
CR_minus_Go_xifti <- ciftiTools::read_xifti("/ncf/hcp/data/analyses/myelin/fmri_test/data/swe_dpx_zTstat_c01.dtseries.nii", brainstructures="all")

summary(CR_minus_Go_xifti)

## Separate data by hemisphere and subcortex
vertex_lh_data <- unlist(CR_minus_Go_xifti$data[1])
vertex_rh_data <- unlist(CR_minus_Go_xifti$data[2])
vertex_subcort_data <- unlist(CR_minus_Go_xifti$data[3])

## Set vertex-wise threshold
v_thresh <- 4.94 

## Threshold xifti data using abs(z) < 4.94
new_xii <- CR_minus_Go_xifti
str(new_xii$data)

# left hemisphere
new_lh_data <- new_xii$data[[1]]
thresh_idx<- which(abs(new_lh_data) < v_thresh)
new_lh_data[thresh_idx] <- 0

# right hemisphere
new_rh_data <- new_xii$data[[2]]
thresh_idx<- which(abs(new_rh_data) < v_thresh)
new_rh_data[thresh_idx] <- 0

# subcortical
new_subcort_data <- new_xii$data[[3]]
thresh_idx<- which(abs(new_subcort_data) < v_thresh)
new_subcort_data[thresh_idx] <- 0

## Create cifti with thresholded data
new_xii$data[[1]] <- new_lh_data
new_xii$data[[2]] <- new_rh_data
new_xii$data[[3]] <-new_subcort_data

## Confirm that xifti format was preserved
is.xifti(new_xii)

## Define output directory for new cifti file
out_dir <- "/ncf/hcp/data/analyses/myelin/fmri_test/data"

## Export new xifti file
ciftiTools::write_xifti(
  new_xii, 
  file.path(out_dir, "swe_dpx_zTstat_c01_thresh.dtseries.nii"), 
)

#############################################################################
##                          Create parcellated cifti                       ##
## counting number of nonzero vertices in each parcel of thresholded image ##
#############################################################################

## Define i/o
input_cifti <- "/ncf/hcp/data/analyses/myelin/fmri_test/data/swe_dpx_zTstat_c01_thresh.dtseries.nii"
output_cifti <- "/ncf/hcp/data/analyses/myelin/fmri_test/data/swe_dpx_zTstat_c01_thresh_nonzero_count.ptseries.nii"
label_file <- "/ncf/hcp/data/analyses/myelin/parcellations/CortexSubcortex_ColeAnticevic_NetPartition_wSubcorGSR_parcels_LR.dlabel.nii"

## Workbench command to create parcellated cifti counting # nonzero vertices
system(paste0("module load connectome-workbench/1.3.2-fasrc01; wb_command -cifti-parcellate ", input_cifti, " ", label_file, " ", "COLUMN ", output_cifti, " -method COUNT_NONZERO"))

## Convert cifti to text
text_output <- "/ncf/hcp/data/analyses/myelin/fmri_test/data/swe_dpx_zTstat_c01_thresh_nonzero_count.ptseries.txt"
system(paste0("module load connectome-workbench/1.3.2-fasrc01; wb_command -cifti-convert -to-text ", output_cifti, " ", text_output))

###########################################################
## Calculate Parcel Size for Cole-Anticevic parcellation ##
###########################################################
detach("package:ciftiTools", unload=TRUE)
library(cifti)

## Read in dlabel file for parcellation
coleAnt_network_dlabel_cifti <- read_cifti("/ncf/hcp/data/analyses/myelin/parcellations/CortexSubcortex_ColeAnticevic_NetPartition_wSubcorGSR_parcels_LR.dlabel.nii", trans_data = FALSE)
cifti_data <- coleAnt_network_dlabel_cifti$data

lookup_table <-coleAnt_network_dlabel_cifti$NamedMap$look_up_table
lookup_table <- as.data.frame(lookup_table)

## Define parcel names using cifti lookup table
parcel_names <- lookup_table$Label[2:719]
parcel_df <- as.data.frame(parcel_names)
parcel_df$parcel_names

cortical_lookup_table <- lookup_table[2:361,]
subcortical_lookup_table <- lookup_table[362:719,]
head(subcortical_lookup_table)
subcort_parcel_index <- unique(subcortical_lookup_table$Key)

##################################### 
## Extract Subcortical Parcel Size ##
##################################### 
subcortical_parcel_size <- rep(NA, length(subcort_parcel_index))

for(i in 1:length(subcort_parcel_index)) {
  parcel <- subcort_parcel_index[i]
  size <- length(which(cifti_data==parcel))
  subcortical_parcel_size [i] <-size
}

hist(subcortical_parcel_size, col="slategray3")

## Note that there are a number of tiny subcortical parcels with < 4 voxels
length(which(subcortical_parcel_size <= 4))

##################################
## Extract Cortical Parcel size ##
##################################

## Use a parcellated time-series (.ptseries) file to determine cortical parcel size ##
coleAnt_cope_cifti <- read_cifti("/ncf/hcp/data/HCD-tfMRI-MultiRunFix/HCD0001305_V1_MR/MNINonLinear/Results/tfMRI_CARIT_AP/tfMRI_CARIT_AP_hp200_s4_level1_hp0_clean_ColeAnticevic.feat/ParcellatedStats/cope4.ptseries.nii", trans_data = FALSE)
parcel_list <- coleAnt_cope_cifti$Parcel
cortical_parcel_size <- rep(NA, length(parcel_list))

for(i in 1:length(parcel_list)) {
  parcel <- unlist(parcel_list[i])
  size <- length(parcel)
  cortical_parcel_size[i] <-size
}

# Remove empty subcortical parcels
cortical_parcel_size <- cortical_parcel_size[1:360]
hist(cortical_parcel_size, col="slategray3")

############################################
## Create data frame integrating all data ##
############################################

## Nonzero count of vertices with significant activation within each parcel
## (this text file was generated in line 79 above)
nonzero_count <- read.table("/ncf/hcp/data/analyses/myelin/fmri_test/data/swe_dpx_zTstat_c01_thresh_nonzero_count.ptseries.txt")

parcel_df$parcel_number <- as.factor(1:length(parcel_df$parcel_names))
parcel_df$parcel_size <- 0
parcel_df$nonzero_count <- nonzero_count$V1[1:718]
parcel_df$percentage <- 0
parcel_df$sig_idx <- 0

## Plug in parcel data
parcel_df$parcel_size[1:360] <- cortical_parcel_size[1:360]
parcel_df$parcel_size[361:718] <- subcortical_parcel_size[1:358]
parcel_df$percentage <- unlist((parcel_df$nonzero_count / parcel_df$parcel_size)) * 100

idx <- which(parcel_df$percentage >=50)
parcel_df$sig_idx[idx] <- 1 

View(parcel_df)

## Export data
write.csv(parcel_df, "/ncf/hcp/data/analyses/myelin/fmri_test/data/ColeAnticevic_CR-Go_parcel_inclusion.csv", row.names=FALSE)
write.table(parcel_df$sig_idx, "/ncf/hcp/data/analyses/myelin/fmri_test/data/ColeAnticevic_CR-Go_parcel_inclusion_sig_index.txt", row.names=FALSE, col.names=FALSE)


## Function to write CIFTI using a vector of parcel values
write_cifti <- function(template_path, brainVar_path, cifti_output_path) {
  
  system(paste0("module load connectome-workbench/1.3.2-fasrc01; ", "wb_command -cifti-convert -from-text ", brainVar_path, " ", template_path, " ", cifti_output_path))
  return(print(cifti_output_path))
}


#############################
## Edit input/output paths ##
#############################
brainVar.path <- "/ncf/hcp/data/analyses/myelin/fmri_test/data/ColeAnticevic_CR-Go_parcel_inclusion_sig_index.txt"
cifti.output.path <- "/ncf/hcp/data/analyses/myelin/fmri_test/data/ColeAnticevic_CR-Go_parcel_inclusion_sig_index.pscalar.nii" 
glasser_template.path <- "/ncf/hcp/data/analyses/myelin/parcellations/Q1-Q6_RelatedValidation210.CorticalAreas_dil_Final_Final_Areas_Group_Colors.32k_fs_LR_template.pscalar.nii"
coleAnticevic_template.path <- "/ncf/hcp/data/analyses/myelin/parcellations/CortexSubcortex_ColeAnticevic_NetPartition_wSubcorGSR_parcels_LR_myelin_template.pscalar.nii"

write_cifti(coleAnticevic_template.path, brainVar.path, cifti.output.path)
